//! The log-structured merge tree.
//!
//! # Shape
//!
//! A write goes to the log, then to an in-memory table. When that table fills
//! it is frozen and a background thread writes it out as a level-0 SSTable.
//! Level 0 files overlap, because each one is a whole memtable's key range;
//! every level below is kept non-overlapping by compaction. Reads consult the
//! memtables, then level 0 newest-first, then one file per deeper level.
//!
//! The bargain is that writes become sequential and cheap while reads become a
//! search over several places. Bloom filters and the non-overlapping invariant
//! below level 0 are what stop that search from being expensive.
//!
//! # Lock order
//!
//! Taken in this order, always, so there is no cycle to deadlock on:
//!
//! ```text
//!   compaction -> manifest -> version -> memtables -> log
//! ```
//!
//! `compaction` is a plain mutex making flush and compaction single-flight.
//! Writers only ever take `memtables` then `log`, which is a suffix of the
//! order, so a writer can never hold a lock a background job is waiting for
//! while waiting for one the background job holds.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::error::{Error, IoContext, Result};
use crate::manifest::{FileMeta, MAX_LEVELS, Manifest, Version};
use crate::memtable::MemTable;
use crate::sstable::{SsTable, SsTableWriter};
use crate::types::{Entry, Mutation, Record};
use crate::wal::{self, SyncPolicy, Wal, WalStats};

/// Tuning for the engine.
#[derive(Debug, Clone)]
pub struct Options {
    /// The data directory.
    pub dir: PathBuf,
    /// Freeze the active memtable once it passes this many bytes.
    pub memtable_bytes: usize,
    /// Target size of an SSTable data block.
    pub block_size: usize,
    /// Bloom filter bits per key.
    pub bits_per_key: usize,
    /// When log bytes are forced to stable storage.
    pub sync_policy: SyncPolicy,
    /// Start a level-0 compaction once this many files accumulate there.
    pub l0_compaction_trigger: usize,
    /// Stall writers once level 0 reaches this many files.
    ///
    /// Level 0 files all have to be consulted on every read, so letting them
    /// pile up without bound turns reads into a linear scan. Blocking the
    /// writer is the honest response: it makes the pressure visible as latency
    /// instead of hiding it as a slow read path later.
    pub l0_stop_writes_trigger: usize,
    /// Byte budget for level 1.
    pub level_base_bytes: u64,
    /// Each level holds this many times more than the one above.
    pub level_multiplier: u64,
    /// Largest SSTable a compaction will produce before starting another.
    pub max_file_bytes: u64,
    /// How many frozen memtables may await flushing before writes stall.
    pub max_immutable: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            dir: PathBuf::from("data"),
            memtable_bytes: 64 << 20,
            block_size: crate::sstable::DEFAULT_BLOCK_SIZE,
            bits_per_key: crate::bloom::DEFAULT_BITS_PER_KEY,
            sync_policy: SyncPolicy::EverySecond,
            l0_compaction_trigger: 4,
            l0_stop_writes_trigger: 12,
            level_base_bytes: 64 << 20,
            level_multiplier: 10,
            max_file_bytes: 64 << 20,
            max_immutable: 2,
        }
    }
}

impl Options {
    /// Byte budget for `level`.
    fn max_bytes_for_level(&self, level: usize) -> u64 {
        if level <= 1 {
            return self.level_base_bytes;
        }
        let mut bytes = self.level_base_bytes;
        for _ in 1..level {
            bytes = bytes.saturating_mul(self.level_multiplier);
        }
        bytes
    }
}

/// The memtables: one being written, plus any frozen ones awaiting flush.
#[derive(Debug)]
struct MemSet {
    active: MemTable,
    /// Frozen tables, newest first, which is the order reads must consult them
    /// in. Each remembers the log that fed it, so that once it reaches disk the
    /// engine knows exactly which log files became redundant.
    immutable: Vec<Frozen>,
}

/// A memtable awaiting flush, paired with the log that backs it.
#[derive(Debug, Clone)]
struct Frozen {
    log_number: u64,
    table: Arc<MemTable>,
}

#[derive(Debug, Default)]
struct Counters {
    writes: AtomicU64,
    reads: AtomicU64,
    flushes: AtomicU64,
    compactions: AtomicU64,
    compaction_bytes_read: AtomicU64,
    compaction_bytes_written: AtomicU64,
    write_stalls: AtomicU64,
    bloom_rejections: AtomicU64,
    sstable_reads: AtomicU64,
}

/// One page of a scan: the live records, and the key to resume from.
pub type ScanPage = (Vec<(Vec<u8>, Record)>, Option<Vec<u8>>);

/// What startup found.
#[derive(Debug, Clone, Default)]
pub struct Recovery {
    /// SSTables named by the manifest.
    pub tables: usize,
    /// Log batches replayed.
    pub log_batches: u64,
    /// Mutations replayed.
    pub log_mutations: u64,
    /// Log files read.
    pub log_files: usize,
    /// Whether an incomplete record was discarded from the end of a log.
    pub truncated: bool,
    /// Orphaned files removed because no manifest referenced them.
    pub orphans_removed: usize,
    /// How long recovery took.
    pub elapsed: Duration,
}

/// Engine counters.
#[derive(Debug, Clone)]
pub struct Stats {
    /// Live keys, estimated. Over-counts keys present at several levels and
    /// tombstones not yet compacted away; an exact count requires a full merge.
    pub keys: u64,
    /// Bytes across all SSTables.
    pub disk_bytes: u64,
    /// Approximate memtable footprint.
    pub memory_bytes: u64,
    /// Files per level.
    pub level_files: Vec<usize>,
    /// Bytes per level.
    pub level_bytes: Vec<u64>,
    /// Write operations.
    pub writes: u64,
    /// Read operations.
    pub reads: u64,
    /// Memtable flushes completed.
    pub flushes: u64,
    /// Compactions completed.
    pub compactions: u64,
    /// Bytes read by compaction.
    pub compaction_bytes_read: u64,
    /// Bytes written by compaction.
    pub compaction_bytes_written: u64,
    /// Times a writer was stalled by level-0 backpressure.
    pub write_stalls: u64,
    /// SSTable lookups skipped by a Bloom filter or key range check.
    pub bloom_rejections: u64,
    /// SSTable lookups that actually read a block.
    pub sstable_reads: u64,
    /// Log counters.
    pub wal: WalStats,
}

/// Shared engine state.
struct Inner {
    opts: Options,
    mem: RwLock<MemSet>,
    log: RwLock<Arc<Wal>>,
    version: RwLock<Arc<Version>>,
    manifest: Mutex<Manifest>,
    /// Open SSTables, keyed by file number. An open table holds its index and
    /// Bloom filter in memory, which is what makes a read cheap; the data
    /// blocks stay on disk.
    tables: RwLock<HashMap<u64, Arc<SsTable>>>,
    /// Makes flush and compaction single-flight.
    compaction: Mutex<()>,
    /// Where the last compaction of each level stopped, so repeated
    /// compactions rotate across the key space instead of rewriting the same
    /// file over and over.
    compact_pointer: Mutex<HashMap<usize, Vec<u8>>>,

    work: Mutex<bool>,
    work_ready: Condvar,
    /// Signalled when level 0 shrinks, waking any stalled writer.
    space_available: Condvar,

    stopping: AtomicBool,
    /// Latches the first background failure. Once the engine cannot flush, it
    /// must stop accepting writes rather than acknowledging data it will lose.
    fatal: Mutex<Option<String>>,

    counters: Counters,
}

/// A log-structured merge tree.
pub struct Db {
    inner: Arc<Inner>,
    background: Mutex<Option<std::thread::JoinHandle<()>>>,
    recovery: Recovery,
}

impl Db {
    /// Opens or creates a database in `opts.dir`.
    pub fn open(opts: Options) -> Result<Db> {
        let started = Instant::now();
        std::fs::create_dir_all(&opts.dir).ctx(&opts.dir)?;

        let mut manifest = Manifest::load(&opts.dir)?;
        let mut rec = Recovery {
            tables: manifest.version.file_count(),
            ..Default::default()
        };

        // Replay every log at or after the one the manifest was written with.
        // Anything older was already folded into an SSTable.
        let mut mem = MemTable::new();
        let logs: Vec<u64> = wal::list_logs(&opts.dir)?
            .into_iter()
            .filter(|n| *n >= manifest.log_number)
            .collect();
        for number in &logs {
            let r = wal::replay(&opts.dir, *number, |seq, muts| {
                for m in muts {
                    mem.insert(m.key, m.entry);
                }
                manifest.last_sequence = manifest.last_sequence.max(seq);
                Ok(())
            })?;
            rec.log_batches += r.batches;
            rec.log_mutations += r.mutations;
            rec.truncated |= r.truncated;
            rec.log_files += 1;
        }

        let log_number = manifest.allocate_file_number();
        let log = Wal::create(
            &opts.dir,
            log_number,
            manifest.last_sequence,
            opts.sync_policy,
        )?;
        manifest.log_number = log_number;

        let version = Arc::new(manifest.version.clone());
        manifest.store(&opts.dir)?;

        // Any .sst the manifest does not name is the debris of a flush or
        // compaction that died before its manifest write. It is unreachable by
        // construction, so removing it is safe and keeps a crash loop from
        // filling the disk.
        rec.orphans_removed = remove_orphans(&opts.dir, &manifest, log_number)?;
        for n in logs {
            if n < log_number {
                wal::remove_log(&opts.dir, n)?;
            }
        }

        let inner = Arc::new(Inner {
            opts,
            mem: RwLock::new(MemSet {
                active: mem,
                immutable: Vec::new(),
            }),
            log: RwLock::new(Arc::new(log)),
            version: RwLock::new(version),
            manifest: Mutex::new(manifest),
            tables: RwLock::new(HashMap::new()),
            compaction: Mutex::new(()),
            compact_pointer: Mutex::new(HashMap::new()),
            work: Mutex::new(false),
            work_ready: Condvar::new(),
            space_available: Condvar::new(),
            stopping: AtomicBool::new(false),
            fatal: Mutex::new(None),
            counters: Counters::default(),
        });

        // A recovered memtable can already be over the threshold, so ask for a
        // flush before serving anything.
        inner.maybe_freeze()?;

        let bg = {
            let inner = Arc::clone(&inner);
            std::thread::Builder::new()
                .name("rfs-lsm-bg".into())
                .spawn(move || inner.background_loop())
                .map_err(Error::RawIo)?
        };

        rec.elapsed = started.elapsed();
        tracing::info!(
            tables = rec.tables,
            log_batches = rec.log_batches,
            log_files = rec.log_files,
            truncated = rec.truncated,
            orphans_removed = rec.orphans_removed,
            took_ms = rec.elapsed.as_millis() as u64,
            "storage engine recovered"
        );
        if rec.truncated {
            tracing::warn!(
                "discarded an incomplete record at the end of the write-ahead log; \
                 this is expected after a crash and the record was never acknowledged to a client"
            );
        }

        Ok(Db {
            inner,
            background: Mutex::new(Some(bg)),
            recovery: rec,
        })
    }

    /// What startup found.
    pub fn recovery(&self) -> &Recovery {
        &self.recovery
    }

    /// Writes one key.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>, expire_at: i64) -> Result<u64> {
        self.write(vec![Mutation::put(key, value, expire_at)])
    }

    /// Deletes one key.
    pub fn delete(&self, key: Vec<u8>) -> Result<u64> {
        self.write(vec![Mutation::delete(key)])
    }

    /// Applies a batch atomically with respect to recovery.
    pub fn write(&self, muts: Vec<Mutation>) -> Result<u64> {
        self.inner.write(muts)
    }

    /// Reads a key as of `now_ms`, resolving tombstones and expiry.
    pub fn get(&self, key: &[u8], now_ms: i64) -> Result<Option<Record>> {
        self.inner.get(key, now_ms)
    }

    /// Returns up to `limit` live records from `start` inclusive, in key order,
    /// along with the key to resume from.
    pub fn scan(&self, start: &[u8], limit: usize, now_ms: i64) -> Result<ScanPage> {
        self.inner.scan(start, limit, now_ms)
    }

    /// Counts live keys exactly by walking the keyspace. This is O(n); see
    /// [`Db::estimated_len`] for the constant-time approximation INFO uses.
    pub fn exact_len(&self, now_ms: i64) -> Result<u64> {
        self.inner.exact_len(now_ms)
    }

    /// Estimates live keys in constant time, over-counting duplicates across
    /// levels and tombstones not yet compacted away.
    pub fn estimated_len(&self) -> u64 {
        self.inner.estimated_len()
    }

    /// Removes every key.
    pub fn flush_all(&self) -> Result<()> {
        self.inner.flush_all()
    }

    /// Forces buffered log records to stable storage.
    pub fn sync(&self) -> Result<()> {
        let log = Arc::clone(&self.inner.log.read().expect("log lock"));
        log.sync_all()
    }

    /// Pushes buffered log records to the kernel without an fsync.
    pub fn flush_log(&self) -> Result<()> {
        let log = Arc::clone(&self.inner.log.read().expect("log lock"));
        log.flush()
    }

    /// Freezes the active memtable and waits for every frozen one to reach
    /// disk. Used by tests and by the explicit flush RPC.
    pub fn flush_memtable(&self) -> Result<()> {
        self.inner.force_freeze()?;
        self.inner.drain_flushes()
    }

    /// Runs compaction until no level is over budget. Used by tests and by the
    /// explicit compact RPC.
    pub fn compact_all(&self) -> Result<()> {
        self.inner.compact_until_idle()
    }

    /// Engine counters.
    pub fn stats(&self, now_ms: i64) -> Result<Stats> {
        self.inner.stats(now_ms)
    }

    /// Stops the background thread and flushes durably.
    pub fn close(&self) -> Result<()> {
        if self.inner.stopping.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        {
            let mut w = self.inner.work.lock().expect("work lock");
            *w = true;
            self.inner.work_ready.notify_all();
            self.inner.space_available.notify_all();
        }
        if let Some(h) = self.background.lock().expect("bg lock").take() {
            let _ = h.join();
        }
        let log = Arc::clone(&self.inner.log.read().expect("log lock"));
        log.close()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Inner {
    fn check_healthy(&self) -> Result<()> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if let Some(msg) = self.fatal.lock().expect("fatal lock").as_ref() {
            return Err(Error::Poisoned(msg.clone()));
        }
        Ok(())
    }

    fn poison(&self, msg: String) {
        let mut f = self.fatal.lock().expect("fatal lock");
        if f.is_none() {
            tracing::error!(error = %msg, "storage engine has failed and is refusing further writes");
            *f = Some(msg);
        }
    }

    fn write(&self, muts: Vec<Mutation>) -> Result<u64> {
        self.check_healthy()?;
        if muts.is_empty() {
            return Ok(0);
        }
        self.await_space()?;

        let seq = {
            // The log append happens while the memtable lock is held, before
            // the mutation is visible to any reader. If it happened outside,
            // two writers to the same key could take sequence numbers in one
            // order and apply to memory in the other: memory would end at one
            // value, replay at the other, and the database would return a
            // different answer after every restart.
            let mut m = self.mem.write().expect("memtable lock");
            let log = Arc::clone(&self.log.read().expect("log lock"));
            let seq = log.append(&muts)?;
            for mu in muts {
                m.active.insert(mu.key, mu.entry);
            }
            seq
        };

        // The fsync happens with no memtable lock held, so one fsync can cover
        // many concurrent writers instead of serialising them behind a lock.
        let log = Arc::clone(&self.log.read().expect("log lock"));
        log.commit(seq)?;

        self.counters.writes.fetch_add(1, Ordering::Relaxed);
        self.maybe_freeze()?;
        Ok(seq)
    }

    /// Blocks while level 0 is too deep or too many memtables await flushing.
    fn await_space(&self) -> Result<()> {
        let l0 = self.version.read().expect("version lock").level(0).len();
        let immutable = self.mem.read().expect("memtable lock").immutable.len();
        if l0 < self.opts.l0_stop_writes_trigger && immutable < self.opts.max_immutable {
            return Ok(());
        }

        self.counters.write_stalls.fetch_add(1, Ordering::Relaxed);
        self.signal_work();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut guard = self.work.lock().expect("work lock");
        loop {
            let l0 = self.version.read().expect("version lock").level(0).len();
            let immutable = self.mem.read().expect("memtable lock").immutable.len();
            if l0 < self.opts.l0_stop_writes_trigger && immutable < self.opts.max_immutable {
                return Ok(());
            }
            if self.stopping.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            if Instant::now() >= deadline {
                // Falling through rather than blocking forever: a stall this
                // long means compaction is not keeping up, and a stuck writer
                // is harder to diagnose than a deep level 0.
                tracing::warn!(
                    l0,
                    immutable,
                    "write stalled for 10s waiting on compaction, proceeding anyway"
                );
                return Ok(());
            }
            let (g, _) = self
                .space_available
                .wait_timeout(guard, Duration::from_millis(50))
                .expect("space condvar");
            guard = g;
        }
    }

    fn maybe_freeze(&self) -> Result<()> {
        let full = {
            let m = self.mem.read().expect("memtable lock");
            m.active.approx_bytes() >= self.opts.memtable_bytes
        };
        if full {
            self.force_freeze()?;
        }
        Ok(())
    }

    /// Freezes the active memtable and starts a fresh log for its replacement.
    ///
    /// Both happen under the same lock. If a new memtable started taking writes
    /// before the new log existed, those writes would be recorded in the log
    /// that is about to be declared superseded, and a crash would lose exactly
    /// the writes made during the gap.
    fn force_freeze(&self) -> Result<()> {
        let mut m = self.mem.write().expect("memtable lock");
        if m.active.is_empty() {
            return Ok(());
        }
        let mut log_guard = self.log.write().expect("log lock");

        let old_log = Arc::clone(&log_guard);
        old_log.flush()?;

        let new_number = {
            let mut man = self.manifest.lock().expect("manifest lock");
            man.allocate_file_number()
        };
        let new_log = Wal::create(
            &self.opts.dir,
            new_number,
            old_log.last_sequence(),
            self.opts.sync_policy,
        )?;

        let frozen = std::mem::replace(&mut m.active, MemTable::new());
        m.immutable.insert(
            0,
            Frozen {
                log_number: old_log.number(),
                table: Arc::new(frozen),
            },
        );
        *log_guard = Arc::new(new_log);

        drop(log_guard);
        drop(m);
        self.signal_work();
        Ok(())
    }

    fn signal_work(&self) {
        let mut w = self.work.lock().expect("work lock");
        *w = true;
        self.work_ready.notify_one();
    }

    fn get(&self, key: &[u8], now_ms: i64) -> Result<Option<Record>> {
        self.counters.reads.fetch_add(1, Ordering::Relaxed);

        // Newest first: the memtable, then frozen memtables, then level 0
        // newest-first, then one file per deeper level. The first level with an
        // opinion wins, and a tombstone is an opinion.
        {
            let m = self.mem.read().expect("memtable lock");
            if let Some(answer) = m.active.lookup(key, now_ms) {
                return Ok(answer);
            }
            for f in &m.immutable {
                if let Some(answer) = f.table.lookup(key, now_ms) {
                    return Ok(answer);
                }
            }
        }

        let version = Arc::clone(&self.version.read().expect("version lock"));
        for f in version.level(0) {
            if let Some(answer) = self.lookup_file(f, key, now_ms)? {
                return Ok(answer);
            }
        }
        for level in 1..version.level_count() {
            let Some(f) = version.find_in_level(level, key) else {
                continue;
            };
            if let Some(answer) = self.lookup_file(f, key, now_ms)? {
                return Ok(answer);
            }
        }
        Ok(None)
    }

    /// Looks in one file. The outer `Option` is "this file has an opinion".
    fn lookup_file(
        &self,
        f: &Arc<FileMeta>,
        key: &[u8],
        now_ms: i64,
    ) -> Result<Option<Option<Record>>> {
        if !f.may_contain(key) {
            self.counters
                .bloom_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let table = self.open_table(f)?;
        if !table.may_contain(key) {
            self.counters
                .bloom_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        self.counters.sstable_reads.fetch_add(1, Ordering::Relaxed);
        match table.get(key)? {
            None => Ok(None),
            Some(Entry::Delete) => Ok(Some(None)),
            Some(Entry::Put(r)) if r.expired(now_ms) => Ok(Some(None)),
            Some(Entry::Put(r)) => Ok(Some(Some(r))),
        }
    }

    fn open_table(&self, f: &Arc<FileMeta>) -> Result<Arc<SsTable>> {
        if let Some(t) = self.tables.read().expect("table cache").get(&f.number) {
            return Ok(Arc::clone(t));
        }
        let table = Arc::new(SsTable::open(f.path(&self.opts.dir))?);
        let mut cache = self.tables.write().expect("table cache");
        // Another thread may have opened it while this one was reading. Keep
        // whichever is already installed so all readers share one handle.
        Ok(Arc::clone(cache.entry(f.number).or_insert(table)))
    }

    /// Returns up to `limit` live records from `start`, plus where to resume.
    ///
    /// Each source contributes at most `limit + 1` entries. That bound is what
    /// makes the scan incremental rather than a whole-database read per page:
    /// any key among the globally smallest N must also be among the smallest N
    /// of the source it came from, so the merge cannot miss one.
    fn scan(&self, start: &[u8], limit: usize, now_ms: i64) -> Result<ScanPage> {
        let limit = limit.max(1);
        let take = limit + 1;
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Record>> =
            std::collections::BTreeMap::new();
        // The merge is only complete up to this key.
        //
        // A source cut off at `take` entries was read no further than its own
        // last key, so beyond the *smallest* such key the merged set may be
        // missing entries that source would have contributed. Emitting past
        // that boundary silently drops keys, which is how a scan loses data
        // without ever erroring.
        let mut boundary: Option<Vec<u8>> = None;
        let note = |k: Option<Vec<u8>>, boundary: &mut Option<Vec<u8>>| {
            if let Some(k) = k {
                match boundary {
                    Some(b) if *b <= k => {}
                    _ => *boundary = Some(k),
                }
            }
        };

        // Collect oldest source first and let newer ones overwrite, so the map
        // ends holding the winning version of every key.
        let version = Arc::clone(&self.version.read().expect("version lock"));
        for level in (1..version.level_count()).rev() {
            for f in version.level(level) {
                let cut = self.collect_from_file(f, start, take, &mut merged)?;
                note(cut, &mut boundary);
            }
        }
        for f in version.level(0).iter().rev() {
            let cut = self.collect_from_file(f, start, take, &mut merged)?;
            note(cut, &mut boundary);
        }
        {
            let m = self.mem.read().expect("memtable lock");
            for fz in m.immutable.iter().rev() {
                let cut = collect_from_memtable(&fz.table, start, take, now_ms, &mut merged);
                note(cut, &mut boundary);
            }
            let cut = collect_from_memtable(&m.active, start, take, now_ms, &mut merged);
            note(cut, &mut boundary);
        }

        let mut out = Vec::with_capacity(limit.min(merged.len()));
        let mut next = None;
        for (k, v) in merged {
            if out.len() == limit {
                next = Some(k);
                break;
            }
            if boundary.as_ref().is_some_and(|b| k > *b) {
                break;
            }
            if let Some(r) = v {
                out.push((k, r));
            }
        }
        if next.is_none() {
            // Resuming just past the boundary is the smallest key strictly
            // greater than it, which guarantees forward progress without
            // needing to know what the next real key is.
            next = boundary.map(successor);
        }
        Ok((out, next))
    }

    /// Copies up to `take` entries at or after `start` out of one file.
    ///
    /// Returns the last key read when the file held more than it was allowed to
    /// contribute, which tells the caller how far the merge can be trusted.
    fn collect_from_file(
        &self,
        f: &Arc<FileMeta>,
        start: &[u8],
        take: usize,
        into: &mut std::collections::BTreeMap<Vec<u8>, Option<Record>>,
    ) -> Result<Option<Vec<u8>>> {
        if f.max_key.as_slice() < start {
            return Ok(None);
        }
        let table = self.open_table(f)?;
        let mut it = table.iter();
        let mut n = 0;
        let mut last = None;
        while let Some((k, e)) = it.next_entry()? {
            if k.as_slice() < start {
                continue;
            }
            if n == take {
                return Ok(last);
            }
            // Expiry is deliberately not applied here. It is resolved once at
            // the end, after the newest version of each key has won, so an
            // expired old version cannot mask a live new one.
            last = Some(k.clone());
            into.insert(
                k,
                match e {
                    Entry::Delete => None,
                    Entry::Put(r) => Some(r),
                },
            );
            n += 1;
        }
        Ok(None)
    }

    /// Counts live keys exactly by walking the whole keyspace.
    ///
    /// This is genuinely O(n) in an LSM tree: the same key can appear at every
    /// level and only a merge can say how many distinct live ones there are.
    /// It exists for tests and for an explicit request; stats uses the cheap
    /// estimate instead, so INFO can never scan the database.
    fn exact_len(&self, now_ms: i64) -> Result<u64> {
        let mut total = 0u64;
        let mut start = Vec::new();
        loop {
            let (page, next) = self.scan(&start, 10_000, now_ms)?;
            total += page.len() as u64;
            match next {
                Some(k) => start = k,
                None => return Ok(total),
            }
        }
    }

    /// Estimates live keys in constant time.
    ///
    /// It sums the entry counts the manifest records plus the memtables, so it
    /// over-counts every key present at more than one level and every tombstone
    /// not yet compacted away. That is the price of an O(1) answer in an LSM
    /// tree, and it is why the number is labelled an estimate wherever it
    /// surfaces.
    fn estimated_len(&self) -> u64 {
        let version = Arc::clone(&self.version.read().expect("version lock"));
        let m = self.mem.read().expect("memtable lock");
        version.total_entries()
            + m.active.len() as u64
            + m.immutable
                .iter()
                .map(|f| f.table.len() as u64)
                .sum::<u64>()
    }

    fn flush_all(&self) -> Result<()> {
        // Every key currently visible is deleted by writing a tombstone for it.
        // Compaction reclaims the space; until then reads see the tombstones and
        // report the keys as gone, which is what matters.
        let mut start = Vec::new();
        loop {
            let (page, next) = self.scan(&start, 10_000, i64::MAX)?;
            if !page.is_empty() {
                let muts: Vec<Mutation> = page
                    .iter()
                    .map(|(k, _)| Mutation::delete(k.clone()))
                    .collect();
                self.write(muts)?;
            }
            match next {
                Some(k) => start = k,
                None => break,
            }
        }
        Ok(())
    }

    fn stats(&self, _now_ms: i64) -> Result<Stats> {
        let version = Arc::clone(&self.version.read().expect("version lock"));
        let log = Arc::clone(&self.log.read().expect("log lock"));
        let memory_bytes = {
            let m = self.mem.read().expect("memtable lock");
            (m.active.approx_bytes()
                + m.immutable
                    .iter()
                    .map(|f| f.table.approx_bytes())
                    .sum::<usize>()) as u64
        };
        Ok(Stats {
            keys: self.estimated_len(),
            disk_bytes: version.total_bytes(),
            memory_bytes,
            level_files: (0..version.level_count())
                .map(|l| version.level(l).len())
                .collect(),
            level_bytes: (0..version.level_count())
                .map(|l| version.level_bytes(l))
                .collect(),
            writes: self.counters.writes.load(Ordering::Relaxed),
            reads: self.counters.reads.load(Ordering::Relaxed),
            flushes: self.counters.flushes.load(Ordering::Relaxed),
            compactions: self.counters.compactions.load(Ordering::Relaxed),
            compaction_bytes_read: self.counters.compaction_bytes_read.load(Ordering::Relaxed),
            compaction_bytes_written: self
                .counters
                .compaction_bytes_written
                .load(Ordering::Relaxed),
            write_stalls: self.counters.write_stalls.load(Ordering::Relaxed),
            bloom_rejections: self.counters.bloom_rejections.load(Ordering::Relaxed),
            sstable_reads: self.counters.sstable_reads.load(Ordering::Relaxed),
            wal: log.stats(),
        })
    }
}

/// Copies up to `take` entries at or after `start` out of a memtable,
/// returning the last key read if it was cut short.
fn collect_from_memtable(
    t: &MemTable,
    start: &[u8],
    take: usize,
    now_ms: i64,
    into: &mut std::collections::BTreeMap<Vec<u8>, Option<Record>>,
) -> Option<Vec<u8>> {
    let mut last = None;
    for (n, (k, e)) in t.range_from(start).enumerate() {
        if n == take {
            return last;
        }
        last = Some(k.clone());
        into.insert(k.clone(), entry_to_live(e, now_ms));
    }
    None
}

/// The smallest key strictly greater than `k`.
///
/// Byte strings order lexicographically with shorter prefixes first, so
/// appending a zero byte produces the immediate successor: nothing can sort
/// between `k` and `k` followed by `0x00`.
fn successor(mut k: Vec<u8>) -> Vec<u8> {
    k.push(0);
    k
}

fn entry_to_live(e: &Entry, now_ms: i64) -> Option<Record> {
    match e {
        Entry::Delete => None,
        Entry::Put(r) if r.expired(now_ms) => None,
        Entry::Put(r) => Some(r.clone()),
    }
}

/// Removes `.sst` and `.log` files no manifest references.
fn remove_orphans(dir: &Path, manifest: &Manifest, current_log: u64) -> Result<usize> {
    let live: std::collections::HashSet<u64> =
        manifest.version.file_numbers().into_iter().collect();
    let mut removed = 0;
    for entry in std::fs::read_dir(dir).ctx(dir)? {
        let entry = entry.ctx(dir)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(num) = name
            .strip_suffix(".sst")
            .and_then(|n| n.parse::<u64>().ok())
        {
            if !live.contains(&num) {
                tracing::warn!(file = name, "removing an sstable no manifest references");
                std::fs::remove_file(entry.path()).ctx(entry.path())?;
                removed += 1;
            }
        } else if name.starts_with("MANIFEST.tmp.") {
            std::fs::remove_file(entry.path()).ctx(entry.path())?;
            removed += 1;
        } else if name
            .strip_suffix(".log")
            .and_then(|n| n.parse::<u64>().ok())
            .is_some_and(|num| num > current_log)
        {
            // A log numbered above the one just opened can only be debris from
            // a crash between allocating a number and recording it.
            std::fs::remove_file(entry.path()).ctx(entry.path())?;
            removed += 1;
        }
    }
    if removed > 0 {
        wal::sync_dir(dir)?;
    }
    Ok(removed)
}

// ---------------------------------------------------------------------------
// Background work: flushing memtables and compacting levels.
// ---------------------------------------------------------------------------

impl Inner {
    /// The background thread.
    ///
    /// One thread does both flushing and compaction, and it always flushes
    /// first. Flushing is what releases memory and unblocks stalled writers,
    /// so letting a long compaction run while frozen memtables pile up would
    /// turn a compaction backlog into a write outage.
    fn background_loop(&self) {
        loop {
            if self.stopping.load(Ordering::Acquire) {
                // Drain what is pending so a clean shutdown does not leave work
                // for the next start to redo from the log.
                let _ = self.drain_flushes();
                return;
            }

            let did_work = match self.do_background_step() {
                Ok(w) => w,
                Err(e) => {
                    self.poison(e.to_string());
                    // Back off rather than spinning on a failing disk.
                    std::thread::sleep(Duration::from_millis(500));
                    false
                }
            };
            if did_work {
                self.space_available.notify_all();
                continue;
            }

            let mut w = self.work.lock().expect("work lock");
            if !*w {
                let (g, _) = self
                    .work_ready
                    .wait_timeout(w, Duration::from_millis(500))
                    .expect("work condvar");
                w = g;
            }
            *w = false;
        }
    }

    fn do_background_step(&self) -> Result<bool> {
        if self.flush_one()? {
            return Ok(true);
        }
        self.compact_once()
    }

    /// Flushes every frozen memtable.
    fn drain_flushes(&self) -> Result<()> {
        while self.flush_one()? {}
        Ok(())
    }

    /// Writes the oldest frozen memtable out as a level-0 SSTable.
    ///
    /// The order is: write the file, fsync it, publish it in the manifest, and
    /// only then drop the memtable and delete the log. Publishing before the
    /// fsync would let a crash leave the manifest pointing at a file that does
    /// not exist; deleting the log before publishing would lose the data if the
    /// manifest write failed.
    fn flush_one(&self) -> Result<bool> {
        let _guard = self.compaction.lock().expect("compaction lock");

        let Some(frozen) = self
            .mem
            .read()
            .expect("memtable lock")
            .immutable
            .last()
            .cloned()
        else {
            return Ok(false);
        };

        let number = {
            let mut man = self.manifest.lock().expect("manifest lock");
            man.allocate_file_number()
        };
        let path = self.opts.dir.join(format!("{number:012}.sst"));
        let mut w =
            SsTableWriter::create_with(&path, self.opts.block_size, self.opts.bits_per_key)?;
        for (k, e) in frozen.table.iter() {
            w.add(k, e)?;
        }
        let meta = w.finish()?;

        if meta.entry_count == 0 {
            // An empty memtable produces nothing worth publishing.
            std::fs::remove_file(&path).ctx(&path)?;
            self.retire_frozen(frozen.log_number)?;
            return Ok(true);
        }

        let file = Arc::new(FileMeta::from_table(number, &meta));
        let new_version = {
            let mut v = (**self.version.read().expect("version lock")).clone();
            v.add_file(0, Arc::clone(&file));
            Arc::new(v)
        };
        {
            let mut man = self.manifest.lock().expect("manifest lock");
            man.version = (*new_version).clone();
            man.last_sequence = man.last_sequence.max(frozen.table.len() as u64);
            man.store(&self.opts.dir)?;
        }
        *self.version.write().expect("version lock") = new_version;

        self.retire_frozen(frozen.log_number)?;
        self.counters.flushes.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            file = number,
            entries = meta.entry_count,
            bytes = meta.file_size,
            "flushed memtable"
        );
        self.signal_work();
        Ok(true)
    }

    /// Drops the flushed memtable and deletes the logs it made redundant.
    fn retire_frozen(&self, log_number: u64) -> Result<()> {
        let still_needed = {
            let mut m = self.mem.write().expect("memtable lock");
            m.immutable.pop();
            m.immutable
                .last()
                .map(|f| f.log_number)
                .unwrap_or_else(|| self.log.read().expect("log lock").number())
        };
        {
            let mut man = self.manifest.lock().expect("manifest lock");
            if man.log_number != still_needed {
                man.log_number = still_needed;
                man.store(&self.opts.dir)?;
            }
        }
        // Only now, with the manifest saying these logs are superseded, is it
        // safe to remove them.
        for n in wal::list_logs(&self.opts.dir)? {
            if n < still_needed && n <= log_number {
                wal::remove_log(&self.opts.dir, n)?;
            }
        }
        self.space_available.notify_all();
        Ok(())
    }

    /// Runs compactions until every level is inside its budget.
    fn compact_until_idle(&self) -> Result<()> {
        self.drain_flushes()?;
        // Bounded so a pathological configuration cannot spin here forever.
        for _ in 0..1000 {
            if !self.compact_once()? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Performs one compaction if any level is over budget.
    fn compact_once(&self) -> Result<bool> {
        let _guard = self.compaction.lock().expect("compaction lock");
        let version = Arc::clone(&self.version.read().expect("version lock"));
        let Some(plan) = self.pick_compaction(&version) else {
            return Ok(false);
        };
        self.run_compaction(plan)?;
        self.counters.compactions.fetch_add(1, Ordering::Relaxed);
        self.space_available.notify_all();
        Ok(true)
    }

    /// Chooses what to compact.
    ///
    /// Level 0 is scored by file count rather than bytes, because its cost is
    /// paid on every read: each level-0 file has to be consulted separately
    /// since their key ranges overlap. Deeper levels are scored by bytes, where
    /// the cost is space amplification rather than read amplification.
    fn pick_compaction(&self, version: &Version) -> Option<Plan> {
        let mut best: Option<(f64, usize)> = None;

        let l0 = version.level(0).len();
        if l0 >= self.opts.l0_compaction_trigger {
            best = Some((l0 as f64 / self.opts.l0_compaction_trigger as f64, 0));
        }
        for level in 1..version.level_count().saturating_sub(1) {
            let bytes = version.level_bytes(level);
            let budget = self.opts.max_bytes_for_level(level);
            if budget == 0 {
                continue;
            }
            let score = bytes as f64 / budget as f64;
            if score > 1.0 && best.is_none_or(|(b, _)| score > b) {
                best = Some((score, level));
            }
        }

        let (_, level) = best?;
        let output_level = level + 1;
        if output_level >= MAX_LEVELS {
            return None;
        }

        // Level 0 files overlap each other, so they must all be compacted
        // together; taking a subset would leave two files at different levels
        // holding different versions of the same key with no ordering between
        // them.
        let inputs: Vec<Arc<FileMeta>> = if level == 0 {
            version.level(0).to_vec()
        } else {
            vec![self.pick_file_from_level(version, level)?]
        };
        if inputs.is_empty() {
            return None;
        }

        let mut from = inputs[0].min_key.clone();
        let mut to = inputs[0].max_key.clone();
        for f in &inputs[1..] {
            if f.min_key < from {
                from = f.min_key.clone();
            }
            if f.max_key > to {
                to = f.max_key.clone();
            }
        }
        let overlapping = version.overlapping(output_level, &from, &to);

        Some(Plan {
            level,
            output_level,
            inputs,
            overlapping,
        })
    }

    /// Picks the next file to compact out of `level`, rotating through the key
    /// space so that repeated compactions spread across the level instead of
    /// hammering its first file.
    fn pick_file_from_level(&self, version: &Version, level: usize) -> Option<Arc<FileMeta>> {
        let files = version.level(level);
        if files.is_empty() {
            return None;
        }
        let pointer = self.compact_pointer.lock().expect("compact pointer");
        let start = pointer.get(&level).cloned().unwrap_or_default();
        drop(pointer);
        files
            .iter()
            .find(|f| f.max_key > start)
            .or_else(|| files.first())
            .cloned()
    }

    /// Merges the plan's inputs into the output level.
    fn run_compaction(&self, plan: Plan) -> Result<()> {
        let output_level = plan.output_level;
        let bottom_most = self.is_bottom_most(output_level);

        // Newest first: the shallower level always wins a tie, and within level
        // 0 the higher file number is newer. The merge keeps the first entry it
        // sees for a key and discards the rest, so this ordering is what decides
        // which version of a key survives.
        let mut sources: Vec<Arc<FileMeta>> = plan.inputs.clone();
        sources.extend(plan.overlapping.iter().cloned());

        let mut tables = Vec::with_capacity(sources.len());
        let mut bytes_read = 0u64;
        for f in &sources {
            bytes_read += f.size;
            tables.push(self.open_table(f)?);
        }
        let mut iters: Vec<Cursor> = Vec::with_capacity(tables.len());
        for t in &tables {
            let mut it = t.iter();
            let head = it.next_entry()?;
            iters.push(Cursor { head, iter: it });
        }

        let version = Arc::clone(&self.version.read().expect("version lock"));
        let mut outputs: Vec<(u64, crate::sstable::TableMeta)> = Vec::new();
        let mut writer: Option<(u64, PathBuf, SsTableWriter)> = None;
        let mut written = 0u64;
        let mut dropped = 0u64;

        loop {
            // Linear scan for the smallest key. With at most a couple of dozen
            // inputs this beats a binary heap: no allocation, no comparison
            // indirection, and the branch predictor handles it well.
            let mut pick: Option<usize> = None;
            for (i, c) in iters.iter().enumerate() {
                let Some((k, _)) = &c.head else { continue };
                match pick {
                    None => pick = Some(i),
                    Some(p) => {
                        let (pk, _) = iters[p].head.as_ref().expect("picked cursor has a head");
                        if k < pk {
                            pick = Some(i);
                        }
                    }
                }
            }
            let Some(winner) = pick else { break };

            let (key, entry) = iters[winner].head.take().expect("winner has a head");
            iters[winner].head = iters[winner].iter.next_entry()?;

            // Every other cursor sitting on the same key holds an older version.
            for (i, c) in iters.iter_mut().enumerate() {
                if i == winner {
                    continue;
                }
                while matches!(&c.head, Some((k, _)) if *k == key) {
                    c.head = c.iter.next_entry()?;
                }
            }

            // A tombstone or an expired record can only be dropped once nothing
            // below could still hold an older value for the key. Dropping it
            // early would let that older value come back to life.
            let obsolete =
                entry.is_delete() || matches!(&entry, Entry::Put(r) if r.expired(now_millis()));
            if obsolete && (bottom_most || !self.key_exists_below(&version, output_level, &key)) {
                dropped += 1;
                continue;
            }

            if writer.is_none() {
                let number = self
                    .manifest
                    .lock()
                    .expect("manifest lock")
                    .allocate_file_number();
                let path = self.opts.dir.join(format!("{number:012}.sst"));
                let w = SsTableWriter::create_with(
                    &path,
                    self.opts.block_size,
                    self.opts.bits_per_key,
                )?;
                writer = Some((number, path, w));
            }
            let (_, _, w) = writer.as_mut().expect("writer exists");
            w.add(&key, &entry)?;
            written += 1;

            // Cutting output into bounded files is what keeps a later
            // compaction of this level from having to rewrite the whole level.
            if w.estimated_size() >= self.opts.max_file_bytes {
                let (number, _, w) = writer.take().expect("writer exists");
                outputs.push((number, w.finish()?));
            }
        }
        if let Some((number, _, w)) = writer.take() {
            outputs.push((number, w.finish()?));
        }

        let bytes_written: u64 = outputs.iter().map(|(_, m)| m.file_size).sum();
        self.counters
            .compaction_bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.counters
            .compaction_bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);

        self.install_compaction(&plan, outputs)?;

        if let Some(last) = plan.inputs.last() {
            self.compact_pointer
                .lock()
                .expect("compact pointer")
                .insert(plan.level, last.max_key.clone());
        }
        tracing::debug!(
            level = plan.level,
            output_level,
            inputs = sources.len(),
            entries_written = written,
            entries_dropped = dropped,
            bytes_read,
            bytes_written,
            "compacted"
        );
        Ok(())
    }

    /// Publishes a compaction's result and retires its inputs.
    fn install_compaction(
        &self,
        plan: &Plan,
        outputs: Vec<(u64, crate::sstable::TableMeta)>,
    ) -> Result<()> {
        let mut v = (**self.version.read().expect("version lock")).clone();
        v.remove_files(
            plan.level,
            &plan.inputs.iter().map(|f| f.number).collect::<Vec<_>>(),
        );
        v.remove_files(
            plan.output_level,
            &plan
                .overlapping
                .iter()
                .map(|f| f.number)
                .collect::<Vec<_>>(),
        );
        for (number, meta) in &outputs {
            v.add_file(
                plan.output_level,
                Arc::new(FileMeta::from_table(*number, meta)),
            );
        }
        // The non-overlapping invariant below level 0 is what makes a read
        // consult one file per level. Checking it here turns a compaction bug
        // into a loud failure instead of silently wrong reads later.
        v.validate(&self.opts.dir)?;
        let new_version = Arc::new(v);

        {
            let mut man = self.manifest.lock().expect("manifest lock");
            man.version = (*new_version).clone();
            man.store(&self.opts.dir)?;
        }
        *self.version.write().expect("version lock") = new_version;

        // Only after the manifest no longer names them can the input files be
        // removed. Reversing this is the classic way to build an engine that
        // loses a level on exactly one unlucky reboot.
        let retired: Vec<u64> = plan
            .inputs
            .iter()
            .chain(plan.overlapping.iter())
            .map(|f| f.number)
            .collect();
        {
            let mut cache = self.tables.write().expect("table cache");
            for n in &retired {
                cache.remove(n);
            }
        }
        for n in &retired {
            let path = self.opts.dir.join(format!("{n:012}.sst"));
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::io(&path, e)),
            }
        }
        wal::sync_dir(&self.opts.dir)?;
        Ok(())
    }

    /// Reports whether `output_level` is the deepest level holding data.
    fn is_bottom_most(&self, output_level: usize) -> bool {
        let v = self.version.read().expect("version lock");
        (output_level + 1..v.level_count()).all(|l| v.level(l).is_empty())
    }

    /// Reports whether any level below `level` could hold `key`.
    fn key_exists_below(&self, version: &Version, level: usize, key: &[u8]) -> bool {
        (level + 1..version.level_count())
            .any(|l| version.level(l).iter().any(|f| f.may_contain(key)))
    }
}

/// One compaction's inputs.
#[derive(Debug)]
struct Plan {
    level: usize,
    output_level: usize,
    inputs: Vec<Arc<FileMeta>>,
    overlapping: Vec<Arc<FileMeta>>,
}

/// One input to the merge, holding the entry it is currently sitting on.
struct Cursor<'a> {
    head: Option<(Vec<u8>, Entry)>,
    iter: crate::sstable::SsTableIter<'a>,
}

/// Wall time in Unix milliseconds, used only to decide whether an expired
/// record can be discarded during compaction.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
