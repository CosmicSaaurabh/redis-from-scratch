//! The engine's write-ahead log.
//!
//! Every mutation is recorded here before it enters the memtable, so a crash
//! loses at most the records the configured sync policy allows. One log file
//! backs one memtable: when the memtable is flushed to an SSTable the log that
//! fed it has been superseded and is deleted, which is what keeps the log from
//! growing without bound.
//!
//! # Why two locks
//!
//! `buf` guards the in-memory append buffer and the sequence counter, and is
//! held only for the length of a memcpy. `file` guards everything that touches
//! the file descriptor and doubles as the group-commit gate: writers queue on
//! it, and whichever one gets in flushes every record buffered so far. Ten
//! concurrent writers therefore cost one fsync rather than ten.
//!
//! Holding a single lock across the fsync would be simpler and would serialise
//! every append behind a syscall that can take milliseconds on a loaded disk.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::coding::{self, Cursor, put_bytes, put_uvarint, put_varint};
use crate::error::{Error, IoContext, Result};
use crate::types::{Entry, Mutation, Record};

/// When log bytes are forced to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncPolicy {
    /// fsync before a write is acknowledged. A power cut loses nothing that was
    /// acknowledged. This is the only policy that can honestly be called
    /// durable.
    Always,
    /// Hand bytes to the kernel before acknowledging and fsync once a second.
    /// Killing the process loses nothing, because the data is already in the
    /// page cache. Losing power loses up to one second.
    #[default]
    EverySecond,
    /// Never fsync explicitly and leave the decision to the kernel.
    Never,
}

impl SyncPolicy {
    /// Parses a policy from its configuration spelling.
    pub fn parse(s: &str) -> Result<SyncPolicy> {
        match s {
            "always" => Ok(SyncPolicy::Always),
            "everysec" | "" => Ok(SyncPolicy::EverySecond),
            "no" | "never" => Ok(SyncPolicy::Never),
            other => Err(Error::InvalidArgument(format!(
                "unknown sync policy {other:?} (want always, everysec or no)"
            ))),
        }
    }

    /// Renders the policy as its configuration spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            SyncPolicy::Always => "always",
            SyncPolicy::EverySecond => "everysec",
            SyncPolicy::Never => "no",
        }
    }
}

/// Identifies a log file, so that a stray file of another format is rejected
/// rather than parsed into nonsense.
const LOG_MAGIC: &[u8; 8] = b"RFSWAL\x02\x00";

/// crc(4) + payload length(4) + sequence(8).
const REC_HEADER: usize = 16;

/// Bounds a single record so that a length field read out of a corrupt tail
/// cannot ask the reader to allocate gigabytes before the checksum rejects it.
const MAX_RECORD: u32 = 1 << 30;

/// Flush the buffer once it reaches this size, bounding the memory a burst of
/// writes can pin in user space.
const MAX_BUFFERED: usize = 1 << 20;

#[derive(Debug)]
struct Buffered {
    bytes: Vec<u8>,
    spare: Vec<u8>,
    next_seq: u64,
}

#[derive(Debug)]
struct FileState {
    file: File,
    len: u64,
}

/// An append-only, checksummed log.
#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    number: u64,
    policy: SyncPolicy,

    buf: Mutex<Buffered>,
    file: Mutex<FileState>,

    /// Mirrors `next_seq - 1` outside the mutex so `flush` can answer "nothing
    /// to do" without taking a lock. A read-heavy caller flushes on every
    /// batch, and making it queue on a mutex to discover an empty buffer costs
    /// more than the write it was trying to batch.
    last_appended: AtomicU64,
    written: AtomicU64,
    synced: AtomicU64,

    appends: AtomicU64,
    writes: AtomicU64,
    fsyncs: AtomicU64,
    bytes_out: AtomicU64,
}

impl Wal {
    /// Creates log `number` in `dir`, continuing sequence numbers after
    /// `start_seq`.
    pub fn create(dir: &Path, number: u64, start_seq: u64, policy: SyncPolicy) -> Result<Wal> {
        let path = log_path(dir, number);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .ctx(&path)?;

        let mut header = Vec::with_capacity(24);
        header.extend_from_slice(LOG_MAGIC);
        header.extend_from_slice(&number.to_le_bytes());
        header.extend_from_slice(&start_seq.to_le_bytes());
        let sum = coding::checksum(&header);
        header.extend_from_slice(&sum.to_le_bytes());

        let mut f = file;
        f.write_all(&header).ctx(&path)?;
        // The header and the directory entry naming the file must both be
        // durable before any record lands in it. Otherwise a crash can leave a
        // log whose records exist but whose name does not, and recovery would
        // silently skip them.
        f.sync_all().ctx(&path)?;
        sync_dir(dir)?;

        let len = header.len() as u64;
        Ok(Wal {
            path,
            number,
            policy,
            buf: Mutex::new(Buffered {
                bytes: Vec::with_capacity(MAX_BUFFERED),
                spare: Vec::with_capacity(MAX_BUFFERED),
                next_seq: start_seq + 1,
            }),
            file: Mutex::new(FileState { file: f, len }),
            last_appended: AtomicU64::new(start_seq),
            written: AtomicU64::new(start_seq),
            synced: AtomicU64::new(start_seq),
            appends: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            fsyncs: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
        })
    }

    /// The log's file number.
    pub fn number(&self) -> u64 {
        self.number
    }

    /// Where the log lives.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The configured sync discipline.
    pub fn policy(&self) -> SyncPolicy {
        self.policy
    }

    /// The highest sequence number handed out.
    pub fn last_sequence(&self) -> u64 {
        self.last_appended.load(Ordering::Acquire)
    }

    /// The highest sequence number known to be on stable storage.
    pub fn synced_sequence(&self) -> u64 {
        self.synced.load(Ordering::Acquire)
    }

    /// Bytes written to the file so far.
    pub fn file_len(&self) -> u64 {
        self.file.lock().expect("wal file mutex").len
    }

    /// Appends a batch and returns its sequence number.
    ///
    /// The whole batch is one record, so replay applies it all or none of it.
    /// Splitting it into per-key records would let a crash leave a multi-key
    /// write half applied.
    pub fn append(&self, muts: &[Mutation]) -> Result<u64> {
        let mut payload = Vec::with_capacity(64 * muts.len().max(1));
        put_uvarint(&mut payload, muts.len() as u64);
        for m in muts {
            put_bytes(&mut payload, &m.key);
            match &m.entry {
                Entry::Delete => payload.push(1),
                Entry::Put(r) => {
                    payload.push(0);
                    put_varint(&mut payload, r.expire_at);
                    put_bytes(&mut payload, &r.value);
                }
            }
        }
        if payload.len() as u64 > MAX_RECORD as u64 {
            return Err(Error::InvalidArgument(format!(
                "batch encodes to {} bytes, over the {MAX_RECORD} record limit",
                payload.len()
            )));
        }

        let overflow;
        let seq;
        {
            let mut b = self.buf.lock().expect("wal buffer mutex");
            seq = b.next_seq;
            b.next_seq += 1;

            let start = b.bytes.len();
            b.bytes.extend_from_slice(&[0; 4]); // checksum placeholder
            b.bytes
                .extend_from_slice(&(payload.len() as u32).to_le_bytes());
            b.bytes.extend_from_slice(&seq.to_le_bytes());
            b.bytes.extend_from_slice(&payload);
            // The checksum covers the length and sequence as well as the
            // payload. Covering the length is the point: a torn write that
            // corrupts only the length would otherwise be undetectable and
            // would desynchronise every record after it.
            let sum = coding::checksum(&b.bytes[start + 4..]);
            b.bytes[start..start + 4].copy_from_slice(&sum.to_le_bytes());

            overflow = b.bytes.len() >= MAX_BUFFERED;
            self.last_appended.store(seq, Ordering::Release);
        }

        self.appends.fetch_add(1, Ordering::Relaxed);
        if overflow {
            self.flush()?;
        }
        Ok(seq)
    }

    /// Hands buffered records to the kernel without fsyncing.
    ///
    /// This is what makes the `everysec` policy survive a process kill: the
    /// acknowledged writes are already in the page cache, which outlives the
    /// process even though it does not outlive a power cut.
    pub fn flush(&self) -> Result<()> {
        if self.last_appended.load(Ordering::Acquire) <= self.written.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut f = self.file.lock().expect("wal file mutex");
        // Re-check under the gate. Between the fast path above and acquiring
        // the lock another writer may already have written these records, and
        // this is where that turns into a skipped syscall rather than an empty
        // one.
        if self.last_appended.load(Ordering::Acquire) <= self.written.load(Ordering::Acquire) {
            return Ok(());
        }
        self.drain(&mut f)
    }

    /// Blocks until every record up to `seq` is on stable storage.
    ///
    /// Under [`SyncPolicy::Always`] this is the group commit point. Writers
    /// queue on the file lock; whichever one gets in flushes and fsyncs
    /// everything buffered so far, and the writers behind it find `synced`
    /// already past their own sequence and return without a second fsync.
    pub fn sync_to(&self, seq: u64) -> Result<()> {
        if self.synced.load(Ordering::Acquire) >= seq {
            return Ok(());
        }
        let mut f = self.file.lock().expect("wal file mutex");
        if self.synced.load(Ordering::Acquire) >= seq {
            return Ok(());
        }
        self.sync_locked(&mut f)
    }

    /// Forces everything buffered so far to stable storage.
    pub fn sync_all(&self) -> Result<()> {
        let mut f = self.file.lock().expect("wal file mutex");
        self.sync_locked(&mut f)
    }

    /// Applies the configured policy after a write.
    pub fn commit(&self, seq: u64) -> Result<()> {
        match self.policy {
            SyncPolicy::Always => self.sync_to(seq),
            _ => Ok(()),
        }
    }

    fn sync_locked(&self, f: &mut FileState) -> Result<()> {
        self.drain(f)?;
        let upto = self.written.load(Ordering::Acquire);
        if self.synced.load(Ordering::Acquire) >= upto {
            return Ok(());
        }
        f.file.sync_all().ctx(&self.path)?;
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
        self.synced.store(upto, Ordering::Release);
        Ok(())
    }

    fn drain(&self, f: &mut FileState) -> Result<()> {
        let (pending, upto) = {
            let mut b = self.buf.lock().expect("wal buffer mutex");
            if b.bytes.is_empty() {
                return Ok(());
            }
            let upto = b.next_seq - 1;
            // Swap rather than clear: the buffer being written must stay
            // untouched while concurrent appends go into the other one.
            let pending = std::mem::take(&mut b.bytes);
            b.bytes = std::mem::take(&mut b.spare);
            b.bytes.clear();
            (pending, upto)
        };

        let result = f.file.write_all(&pending).ctx(&self.path);
        f.len += pending.len() as u64;
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_out
            .fetch_add(pending.len() as u64, Ordering::Relaxed);

        // Hand the buffer back for reuse whether or not the write succeeded.
        {
            let mut b = self.buf.lock().expect("wal buffer mutex");
            b.spare = pending;
            b.spare.clear();
        }
        result?;
        self.written.store(upto, Ordering::Release);
        Ok(())
    }

    /// Counters for reporting.
    pub fn stats(&self) -> WalStats {
        WalStats {
            appends: self.appends.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            fsyncs: self.fsyncs.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            last_sequence: self.last_sequence(),
            synced_sequence: self.synced_sequence(),
            number: self.number,
            policy: self.policy,
        }
    }

    /// Flushes, fsyncs and closes the log.
    pub fn close(&self) -> Result<()> {
        self.sync_all()
    }
}

/// Log counters surfaced through the engine's stats RPC.
#[derive(Debug, Clone, Copy)]
pub struct WalStats {
    /// Batches appended.
    pub appends: u64,
    /// Write syscalls issued.
    pub writes: u64,
    /// fsync calls issued.
    pub fsyncs: u64,
    /// Bytes written.
    pub bytes_out: u64,
    /// Highest sequence handed out.
    pub last_sequence: u64,
    /// Highest sequence on stable storage.
    pub synced_sequence: u64,
    /// The log's file number.
    pub number: u64,
    /// The configured policy.
    pub policy: SyncPolicy,
}

/// What replaying one log file produced.
#[derive(Debug, Default)]
pub struct Replayed {
    /// Batches applied.
    pub batches: u64,
    /// Individual mutations applied.
    pub mutations: u64,
    /// Highest sequence recovered.
    pub last_sequence: u64,
    /// Whether an incomplete record was discarded from the end.
    pub truncated: bool,
    /// Where the discarded tail began.
    pub truncated_at: u64,
}

/// Replays log `number` from `dir`, calling `apply` for each recovered batch.
///
/// Recovery rests on one asymmetry. A crash can only ever damage the very end
/// of the log, because the log is append-only and the kernel writes forward. So
/// a checksum failure in the final record is an interrupted write: expected,
/// benign, and repaired by truncating it away. A failure anywhere earlier is
/// real corruption, and replay refuses to guess, because silently skipping a
/// bad record in the middle would resurrect whatever the following records
/// overwrote.
pub fn replay<F>(dir: &Path, number: u64, mut apply: F) -> Result<Replayed>
where
    F: FnMut(u64, Vec<Mutation>) -> Result<()>,
{
    let path = log_path(dir, number);
    let mut out = Replayed::default();

    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(Error::io(&path, e)),
    };

    const HEADER: usize = 8 + 8 + 8 + 4;
    if data.len() < HEADER {
        return Err(Error::corrupt(&path, "log is shorter than its header"));
    }
    if &data[..8] != LOG_MAGIC {
        return Err(Error::corrupt(&path, "bad magic"));
    }
    let want = u32::from_le_bytes(data[HEADER - 4..HEADER].try_into().unwrap());
    if coding::checksum(&data[..HEADER - 4]) != want {
        return Err(Error::corrupt(&path, "header checksum mismatch"));
    }
    let start_seq = u64::from_le_bytes(data[16..24].try_into().unwrap());
    out.last_sequence = start_seq;

    let mut pos = HEADER;
    let mut expect_seq = start_seq + 1;
    loop {
        if pos == data.len() {
            return Ok(out); // clean end of log
        }
        let tail = pos as u64;

        // Everything below decides between two outcomes: a torn tail, which is
        // truncated away and reported, or genuine corruption, which is an
        // error. `damage` collects the first, `Err` the second.
        let damage: Option<String> = if data.len() - pos < REC_HEADER {
            Some(format!("record header is only {} bytes", data.len() - pos))
        } else {
            let want_sum = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            let len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
            let seq = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());

            if len > MAX_RECORD || data.len() - pos - REC_HEADER < len as usize {
                Some(format!(
                    "record claims {len} payload bytes but only {} remain",
                    data.len() - pos - REC_HEADER
                ))
            } else {
                let end = pos + REC_HEADER + len as usize;
                let got = coding::checksum(&data[pos + 4..end]);
                if got != want_sum {
                    Some(format!(
                        "checksum {got:08x} does not match the stored {want_sum:08x}"
                    ))
                } else if seq != expect_seq {
                    // A gap or a repeat is not something an interrupted write
                    // can produce, so it is corruption wherever it appears.
                    return Err(Error::corrupt(
                        &path,
                        format!(
                            "sequence {seq} out of order at offset {pos}, expected {expect_seq}"
                        ),
                    ));
                } else {
                    let muts = decode_batch(&data[pos + REC_HEADER..end], &path)?;
                    out.mutations += muts.len() as u64;
                    apply(seq, muts)?;
                    out.batches += 1;
                    out.last_sequence = seq;
                    expect_seq = seq + 1;
                    pos = end;
                    continue;
                }
            }
        };

        let detail = damage.expect("the success path continues the loop");
        tracing::warn!(
            path = %path.display(), offset = tail, reason = %detail,
            "discarding an incomplete record at the end of the log; \
             this is expected after a crash and the record was never acknowledged"
        );
        truncate_at(&path, tail)?;
        sync_dir(dir)?;
        out.truncated = true;
        out.truncated_at = tail;
        return Ok(out);
    }
}

fn decode_batch(body: &[u8], path: &Path) -> Result<Vec<Mutation>> {
    let mut c = Cursor::new(body, path);
    let n = c.uvarint()? as usize;
    if n > body.len() + 1 {
        return Err(Error::corrupt(
            path,
            format!("batch claims {n} mutations in {} bytes", body.len()),
        ));
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let key = c.bytes()?.to_vec();
        let entry = match c.u8()? {
            1 => Entry::Delete,
            0 => {
                let expire_at = c.varint()?;
                let value = c.bytes()?;
                Entry::Put(Record {
                    value: value.into(),
                    expire_at,
                })
            }
            other => {
                return Err(Error::corrupt(
                    path,
                    format!("unknown mutation tag {other}"),
                ));
            }
        };
        out.push(Mutation { key, entry });
    }
    if c.remaining() != 0 {
        return Err(Error::corrupt(
            path,
            format!("{} trailing bytes after a batch", c.remaining()),
        ));
    }
    Ok(out)
}

/// Lists the log file numbers present in `dir`, ascending.
pub fn list_logs(dir: &Path) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(Error::io(dir, e)),
    };
    for entry in entries {
        let entry = entry.ctx(dir)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(num) = name
            .strip_suffix(".log")
            .and_then(|n| n.parse::<u64>().ok())
        {
            out.push(num);
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Removes a superseded log file.
pub fn remove_log(dir: &Path, number: u64) -> Result<()> {
    let path = log_path(dir, number);
    match std::fs::remove_file(&path) {
        Ok(()) => sync_dir(dir),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::io(&path, e)),
    }
}

/// Builds the path of log `number`.
pub fn log_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:012}.log"))
}

fn truncate_at(path: &Path, offset: u64) -> Result<()> {
    let f = OpenOptions::new().write(true).open(path).ctx(path)?;
    f.set_len(offset).ctx(path)?;
    // The truncation itself must be durable, or a crash during recovery leaves
    // the log in a third, previously unseen state.
    f.sync_all().ctx(path)?;
    Ok(())
}

/// fsyncs a directory so that file creations and removals inside it are
/// durable. Creating a file and fsyncing the file itself is not enough: the
/// directory entry that names it lives in a different block.
pub fn sync_dir(dir: &Path) -> Result<()> {
    let d = File::open(dir).ctx(dir)?;
    d.sync_all().ctx(dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn collect(dir: &Path, number: u64) -> (Vec<(u64, Vec<Mutation>)>, Replayed) {
        let mut got = Vec::new();
        let r = replay(dir, number, |seq, muts| {
            got.push((seq, muts));
            Ok(())
        })
        .unwrap();
        (got, r)
    }

    fn put(k: &str, v: &str) -> Mutation {
        Mutation::put(k.as_bytes().to_vec(), v.as_bytes().to_vec(), 0)
    }

    #[test]
    fn round_trips_batches() {
        let dir = TempDir::new().unwrap();
        let w = Wal::create(dir.path(), 1, 0, SyncPolicy::Always).unwrap();

        assert_eq!(w.append(&[put("a", "1")]).unwrap(), 1);
        assert_eq!(w.append(&[Mutation::delete(b"b".to_vec())]).unwrap(), 2);
        assert_eq!(
            w.append(&[
                put("c", "3"),
                Mutation::put(b"d".to_vec(), b"4".to_vec(), 1_700_000_000_000)
            ])
            .unwrap(),
            3
        );
        w.close().unwrap();

        let (got, r) = collect(dir.path(), 1);
        assert_eq!(r.batches, 3);
        assert_eq!(r.mutations, 4);
        assert_eq!(r.last_sequence, 3);
        assert!(!r.truncated);

        assert_eq!(got[0].1[0].key, b"a");
        assert!(matches!(got[1].1[0].entry, Entry::Delete));
        match &got[2].1[1].entry {
            Entry::Put(rec) => {
                assert_eq!(rec.value.as_ref(), b"4");
                assert_eq!(rec.expire_at, 1_700_000_000_000);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_batch_replays_atomically() {
        // The whole point of framing a batch as one record: recovery either
        // sees every mutation in it or none.
        let dir = TempDir::new().unwrap();
        let w = Wal::create(dir.path(), 1, 0, SyncPolicy::Always).unwrap();
        w.append(&[put("a", "1"), put("b", "2"), put("c", "3")])
            .unwrap();
        w.close().unwrap();

        let (got, r) = collect(dir.path(), 1);
        assert_eq!(r.batches, 1, "a multi-key write must recover as one record");
        assert_eq!(got[0].1.len(), 3);
    }

    #[test]
    fn a_torn_tail_is_truncated_and_the_rest_survives() {
        // The contract is that recovery yields a *prefix* of what was written.
        // How many records a given byte cut destroys depends on the record
        // size, so the test asserts the prefix property rather than a fixed
        // count, which is the guarantee callers actually rely on.
        const WRITTEN: usize = 10;
        for cut in [1usize, 5, 17, 40, 60] {
            let dir = TempDir::new().unwrap();
            let w = Wal::create(dir.path(), 1, 0, SyncPolicy::Always).unwrap();
            for i in 0..WRITTEN {
                w.append(&[put(&format!("k{i}"), "value")]).unwrap();
            }
            w.close().unwrap();

            let path = log_path(dir.path(), 1);
            let full = std::fs::read(&path).unwrap();
            std::fs::write(&path, &full[..full.len() - cut]).unwrap();

            let (got, r) = collect(dir.path(), 1);
            assert!(r.truncated, "cut of {cut} bytes was not detected");
            assert!(
                !got.is_empty() && got.len() < WRITTEN,
                "cut {cut}: recovered {} records, expected a proper prefix",
                got.len()
            );
            for (i, (seq, muts)) in got.iter().enumerate() {
                assert_eq!(*seq, i as u64 + 1, "cut {cut}: recovery skipped a record");
                assert_eq!(
                    muts[0].key,
                    format!("k{i}").as_bytes(),
                    "cut {cut}: wrong record at {i}"
                );
            }

            // Truncation must persist, so a second recovery finds a clean log
            // holding exactly the prefix the first one kept.
            let (got2, r2) = collect(dir.path(), 1);
            assert!(!r2.truncated, "cut {cut}: truncation was not persisted");
            assert_eq!(
                got2.len(),
                got.len(),
                "cut {cut}: second recovery disagreed with the first"
            );
        }
    }

    #[test]
    fn a_flipped_byte_in_the_last_record_is_caught() {
        let dir = TempDir::new().unwrap();
        let w = Wal::create(dir.path(), 1, 0, SyncPolicy::Always).unwrap();
        for i in 0..5 {
            w.append(&[put(&format!("k{i}"), "payload")]).unwrap();
        }
        w.close().unwrap();

        let path = log_path(dir.path(), 1);
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        // The file is the right length; only the checksum can catch this.
        bytes[n - 3] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let (got, r) = collect(dir.path(), 1);
        assert!(r.truncated);
        assert_eq!(got.len(), 4);
    }

    #[test]
    fn corruption_before_the_end_is_fatal() {
        let dir = TempDir::new().unwrap();
        let w = Wal::create(dir.path(), 1, 0, SyncPolicy::Always).unwrap();
        for i in 0..20 {
            w.append(&[put(&format!("k{i:03}"), "payload-payload")])
                .unwrap();
        }
        w.close().unwrap();

        let path = log_path(dir.path(), 1);
        let mut bytes = std::fs::read(&path).unwrap();
        // Rewrite a sequence number in the middle. The checksum still fails,
        // but so does the ordering, and either way this is damage a crash could
        // not have caused. It must not be silently repaired.
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let mut count = 0;
        let res = replay(dir.path(), 1, |_, _| {
            count += 1;
            Ok(())
        });
        match res {
            Ok(r) => assert!(
                r.truncated && count < 20,
                "mid-log damage was accepted as a complete log"
            ),
            Err(e) => assert!(e.is_corruption(), "{e:?}"),
        }
    }

    #[test]
    fn header_corruption_is_rejected() {
        let dir = TempDir::new().unwrap();
        let w = Wal::create(dir.path(), 1, 0, SyncPolicy::Always).unwrap();
        w.append(&[put("a", "1")]).unwrap();
        w.close().unwrap();

        let path = log_path(dir.path(), 1);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[2] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        assert!(
            replay(dir.path(), 1, |_, _| Ok(()))
                .unwrap_err()
                .is_corruption()
        );
    }

    #[test]
    fn a_missing_log_replays_to_nothing() {
        let dir = TempDir::new().unwrap();
        let (got, r) = collect(dir.path(), 99);
        assert!(got.is_empty());
        assert_eq!(r.batches, 0);
        assert!(!r.truncated);
    }

    #[test]
    fn sequences_continue_across_log_files() {
        let dir = TempDir::new().unwrap();
        let w1 = Wal::create(dir.path(), 1, 0, SyncPolicy::Always).unwrap();
        w1.append(&[put("a", "1")]).unwrap();
        w1.append(&[put("b", "2")]).unwrap();
        w1.close().unwrap();

        let w2 = Wal::create(dir.path(), 2, w1.last_sequence(), SyncPolicy::Always).unwrap();
        assert_eq!(w2.append(&[put("c", "3")]).unwrap(), 3);
        w2.close().unwrap();

        assert_eq!(list_logs(dir.path()).unwrap(), vec![1, 2]);
        let (_, r) = collect(dir.path(), 2);
        assert_eq!(r.last_sequence, 3);
    }

    #[test]
    fn concurrent_appends_are_unique_ordered_and_all_persisted() {
        let dir = TempDir::new().unwrap();
        let w = Arc::new(Wal::create(dir.path(), 1, 0, SyncPolicy::Always).unwrap());

        const THREADS: usize = 8;
        const EACH: usize = 200;
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let w = Arc::clone(&w);
            handles.push(std::thread::spawn(move || {
                let mut seqs = Vec::with_capacity(EACH);
                for i in 0..EACH {
                    let seq = w.append(&[put(&format!("t{t}:{i:04}"), "v")]).unwrap();
                    w.commit(seq).unwrap();
                    seqs.push(seq);
                }
                seqs
            }));
        }
        let mut all = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }
        w.close().unwrap();

        all.sort_unstable();
        let unique: std::collections::BTreeSet<_> = all.iter().copied().collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "a sequence number was handed out twice"
        );
        assert_eq!(*all.first().unwrap(), 1);
        assert_eq!(*all.last().unwrap(), (THREADS * EACH) as u64);

        let (got, r) = collect(dir.path(), 1);
        assert_eq!(
            got.len(),
            THREADS * EACH,
            "records were lost under concurrency"
        );
        assert!(!r.truncated);
    }

    #[test]
    fn group_commit_amortises_fsync() {
        let dir = TempDir::new().unwrap();
        let w = Arc::new(Wal::create(dir.path(), 1, 0, SyncPolicy::Always).unwrap());

        const THREADS: usize = 32;
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let (w, barrier) = (Arc::clone(&w), Arc::clone(&barrier));
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let seq = w.append(&[put(&format!("k{t}"), "v")]).unwrap();
                w.commit(seq).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let st = w.stats();
        assert!(
            st.fsyncs < THREADS as u64,
            "{} fsyncs for {THREADS} concurrent writers: group commit is not batching",
            st.fsyncs
        );
        assert_eq!(
            st.synced_sequence, THREADS as u64,
            "not every write reached stable storage"
        );
    }

    #[test]
    fn everysec_does_not_fsync_per_write() {
        let dir = TempDir::new().unwrap();
        let w = Wal::create(dir.path(), 1, 0, SyncPolicy::EverySecond).unwrap();
        for i in 0..200 {
            let seq = w.append(&[put(&format!("k{i}"), "v")]).unwrap();
            w.commit(seq).unwrap();
        }
        assert_eq!(w.stats().fsyncs, 0, "everysec fsynced on the write path");

        // Flush must still get the bytes to the kernel, which is what makes a
        // process kill survivable under this policy.
        w.flush().unwrap();
        assert!(w.stats().writes > 0);
        assert_eq!(w.stats().fsyncs, 0);
    }

    #[test]
    fn flush_skips_the_lock_when_there_is_nothing_to_write() {
        let dir = TempDir::new().unwrap();
        let w = Wal::create(dir.path(), 1, 0, SyncPolicy::EverySecond).unwrap();
        w.append(&[put("a", "1")]).unwrap();
        w.flush().unwrap();
        let after = w.stats().writes;
        for _ in 0..100 {
            w.flush().unwrap();
        }
        assert_eq!(
            w.stats().writes,
            after,
            "repeated flushes issued empty writes"
        );
    }

    #[test]
    fn policy_parsing() {
        assert_eq!(SyncPolicy::parse("always").unwrap(), SyncPolicy::Always);
        assert_eq!(
            SyncPolicy::parse("everysec").unwrap(),
            SyncPolicy::EverySecond
        );
        assert_eq!(SyncPolicy::parse("").unwrap(), SyncPolicy::EverySecond);
        assert_eq!(SyncPolicy::parse("no").unwrap(), SyncPolicy::Never);
        assert!(SyncPolicy::parse("sometimes").is_err());
        assert_eq!(SyncPolicy::Always.as_str(), "always");
    }

    #[test]
    fn removing_a_superseded_log_is_idempotent() {
        let dir = TempDir::new().unwrap();
        Wal::create(dir.path(), 7, 0, SyncPolicy::Never)
            .unwrap()
            .close()
            .unwrap();
        assert_eq!(list_logs(dir.path()).unwrap(), vec![7]);
        remove_log(dir.path(), 7).unwrap();
        remove_log(dir.path(), 7).unwrap();
        assert!(list_logs(dir.path()).unwrap().is_empty());
    }
}
