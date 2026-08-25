//! Sorted string tables: the immutable on-disk files an LSM tree is made of.
//!
//! # File layout
//!
//! ```text
//!   data block 0        entries, sorted, ~4 KiB
//!   data block 1
//!   ...
//!   bloom block         one filter over every key in the file
//!   index block         min/max key, then one (last key, offset, length) per data block
//!   footer              fixed 56 bytes: where the bloom and index blocks are
//! ```
//!
//! The footer is last and fixed size so a reader can find everything else with
//! one read from a known offset, without scanning. The index and bloom blocks
//! sit after the data because a writer streams data out before it knows how
//! many entries there will be, and rewriting the head of a finished file would
//! turn an append into a read-modify-write.
//!
//! Every block carries its own CRC-32C. A checksum on the file as a whole would
//! be cheaper to write and useless to read: verifying it would mean reading the
//! entire file to answer one point lookup.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::bloom::{self, Bloom, DEFAULT_BITS_PER_KEY};
use crate::coding::{self, Cursor, put_bytes, put_uvarint, put_varint};
use crate::error::{Error, IoContext, Result};
use crate::types::{Entry, Record};

/// Identifies a redis-from-scratch SSTable.
const MAGIC: u64 = 0x5246_5353_5442_3031;

/// Bumped when the layout changes incompatibly.
pub const FORMAT_VERSION: u32 = 1;

/// Fixed footer size: five u64 offsets, a version, a checksum and the magic.
const FOOTER_SIZE: usize = 8 * 5 + 4 + 4 + 8;

/// Target uncompressed size of a data block.
///
/// Four kilobytes matches a typical filesystem block, so a point lookup that
/// misses the block cache costs one physical read rather than a partial one
/// plus a second for the remainder.
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

/// Tag byte distinguishing a value from a tombstone.
const TAG_PUT: u8 = 0;
const TAG_DELETE: u8 = 1;

/// Where one data block lives, and the largest key inside it.
#[derive(Debug, Clone)]
struct BlockHandle {
    /// The last key in the block. Binary searching these locates the only
    /// block that can contain a given key.
    last_key: Vec<u8>,
    offset: u64,
    length: u32,
}

/// Summary of a finished SSTable, recorded in the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMeta {
    /// The file number this table was written as.
    pub file_number: u64,
    /// Size on disk in bytes.
    pub file_size: u64,
    /// Number of entries, tombstones included.
    pub entry_count: u64,
    /// Smallest key in the file.
    pub min_key: Vec<u8>,
    /// Largest key in the file.
    pub max_key: Vec<u8>,
}

impl TableMeta {
    /// Reports whether this file's key range overlaps `[from, to]`.
    pub fn overlaps(&self, from: &[u8], to: &[u8]) -> bool {
        self.min_key.as_slice() <= to && from <= self.max_key.as_slice()
    }

    /// Reports whether `key` could be in this file's range.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.min_key.as_slice() <= key && key <= self.max_key.as_slice()
    }
}

/// Writes an SSTable.
///
/// Keys must be added in ascending order; the writer asserts it rather than
/// sorting, because a caller that hands it unsorted keys has a bug the writer
/// cannot fix and hiding it would produce a file whose index lies.
pub struct SsTableWriter {
    path: PathBuf,
    file: BufWriter<File>,
    offset: u64,

    block: Vec<u8>,
    block_entries: u32,
    block_size: usize,

    index: Vec<BlockHandle>,
    key_hashes: Vec<u64>,
    bits_per_key: usize,

    entry_count: u64,
    min_key: Option<Vec<u8>>,
    last_key: Vec<u8>,
    finished: bool,
}

impl SsTableWriter {
    /// Creates a new table at `path`.
    pub fn create(path: impl Into<PathBuf>) -> Result<Self> {
        Self::create_with(path, DEFAULT_BLOCK_SIZE, DEFAULT_BITS_PER_KEY)
    }

    /// Creates a table with explicit tuning.
    pub fn create_with(
        path: impl Into<PathBuf>,
        block_size: usize,
        bits_per_key: usize,
    ) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .ctx(&path)?;
        Ok(SsTableWriter {
            file: BufWriter::with_capacity(256 * 1024, file),
            path,
            offset: 0,
            block: Vec::with_capacity(block_size + 1024),
            block_entries: 0,
            block_size: block_size.max(512),
            index: Vec::new(),
            key_hashes: Vec::new(),
            bits_per_key,
            entry_count: 0,
            min_key: None,
            last_key: Vec::new(),
            finished: false,
        })
    }

    /// Number of entries written so far.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Bytes written so far, plus whatever is buffered in the current block.
    ///
    /// Compaction uses this to decide when to cut one output file and start
    /// another. It excludes the index and Bloom blocks, which are not known
    /// until the file is finished, so it slightly understates the final size.
    pub fn estimated_size(&self) -> u64 {
        self.offset + self.block.len() as u64
    }

    /// Appends one entry. Keys must ascend strictly.
    pub fn add(&mut self, key: &[u8], entry: &Entry) -> Result<()> {
        if self.entry_count > 0 && key <= self.last_key.as_slice() {
            return Err(Error::InvalidArgument(format!(
                "sstable keys must ascend: {:?} came after {:?}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(&self.last_key)
            )));
        }
        if self.min_key.is_none() {
            self.min_key = Some(key.to_vec());
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.key_hashes.push(bloom::hash_key(key));

        encode_entry(&mut self.block, key, entry);
        self.block_entries += 1;
        self.entry_count += 1;

        // A block is cut once it passes the target rather than before, so a
        // single entry larger than the block size still gets a block of its
        // own instead of being impossible to write.
        if self.block.len() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block_entries == 0 {
            return Ok(());
        }
        self.block
            .extend_from_slice(&self.block_entries.to_le_bytes());
        let sum = coding::checksum(&self.block);
        self.block.extend_from_slice(&sum.to_le_bytes());

        self.file.write_all(&self.block).ctx(&self.path)?;
        self.index.push(BlockHandle {
            last_key: self.last_key.clone(),
            offset: self.offset,
            length: self.block.len() as u32,
        });
        self.offset += self.block.len() as u64;
        self.block.clear();
        self.block_entries = 0;
        Ok(())
    }

    /// Finishes the table, flushes it to stable storage and returns its
    /// summary.
    ///
    /// The file is fsynced here rather than left to the page cache. An SSTable
    /// only becomes visible when the manifest names it, and the manifest write
    /// must not be able to reach the disk before the file it points at.
    pub fn finish(mut self) -> Result<TableMeta> {
        self.flush_block()?;
        self.finished = true;

        let bloom = Bloom::from_hashes(&self.key_hashes, self.bits_per_key);
        let bloom_offset = self.offset;
        let mut bloom_block = Vec::with_capacity(bloom.bits().len() + 8);
        bloom_block.extend_from_slice(&bloom.hashes().to_le_bytes());
        bloom_block.extend_from_slice(bloom.bits());
        let sum = coding::checksum(&bloom_block);
        bloom_block.extend_from_slice(&sum.to_le_bytes());
        self.file.write_all(&bloom_block).ctx(&self.path)?;
        self.offset += bloom_block.len() as u64;

        let min_key = self.min_key.clone().unwrap_or_default();
        let max_key = self.last_key.clone();

        let index_offset = self.offset;
        let mut index_block = Vec::with_capacity(self.index.len() * 32 + 64);
        put_bytes(&mut index_block, &min_key);
        put_bytes(&mut index_block, &max_key);
        put_uvarint(&mut index_block, self.index.len() as u64);
        for h in &self.index {
            put_bytes(&mut index_block, &h.last_key);
            put_uvarint(&mut index_block, h.offset);
            put_uvarint(&mut index_block, h.length as u64);
        }
        let sum = coding::checksum(&index_block);
        index_block.extend_from_slice(&sum.to_le_bytes());
        self.file.write_all(&index_block).ctx(&self.path)?;
        self.offset += index_block.len() as u64;

        let mut footer = Vec::with_capacity(FOOTER_SIZE);
        footer.extend_from_slice(&bloom_offset.to_le_bytes());
        footer.extend_from_slice(&(bloom_block.len() as u64).to_le_bytes());
        footer.extend_from_slice(&index_offset.to_le_bytes());
        footer.extend_from_slice(&(index_block.len() as u64).to_le_bytes());
        footer.extend_from_slice(&self.entry_count.to_le_bytes());
        footer.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let sum = coding::checksum(&footer);
        footer.extend_from_slice(&sum.to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        debug_assert_eq!(footer.len(), FOOTER_SIZE);
        self.file.write_all(&footer).ctx(&self.path)?;
        self.offset += footer.len() as u64;

        // BufWriter::into_inner would move the file out of self, which the
        // borrow checker refuses because SsTableWriter implements Drop and Drop
        // must still see a whole value. Flushing and syncing through a
        // reference does the same work without the move.
        self.file.flush().ctx(&self.path)?;
        self.file.get_ref().sync_all().ctx(&self.path)?;

        Ok(TableMeta {
            file_number: 0,
            file_size: self.offset,
            entry_count: self.entry_count,
            min_key,
            max_key,
        })
    }
}

impl Drop for SsTableWriter {
    fn drop(&mut self) {
        // An abandoned writer leaves a partial file that nothing will ever
        // reference. Removing it here keeps a repeatedly failing compaction
        // from filling the disk with orphans.
        if !self.finished {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn encode_entry(out: &mut Vec<u8>, key: &[u8], entry: &Entry) {
    put_bytes(out, key);
    match entry {
        Entry::Delete => {
            out.push(TAG_DELETE);
        }
        Entry::Put(r) => {
            out.push(TAG_PUT);
            put_varint(out, r.expire_at);
            put_bytes(out, &r.value);
        }
    }
}

fn decode_entry<'a>(c: &mut Cursor<'a>) -> Result<(&'a [u8], Entry)> {
    let key = c.bytes()?;
    let tag = c.u8()?;
    match tag {
        TAG_DELETE => Ok((key, Entry::Delete)),
        TAG_PUT => {
            let expire_at = c.varint()?;
            let value = c.bytes()?;
            Ok((
                key,
                Entry::Put(Record {
                    value: value.into(),
                    expire_at,
                }),
            ))
        }
        other => Err(Error::InvalidArgument(format!(
            "unknown sstable entry tag {other}"
        ))),
    }
}

/// A readable SSTable.
///
/// Opening one loads the index and the Bloom filter into memory and leaves the
/// data blocks on disk. That split is the whole point: the index and filter are
/// small and hot, the data is large and cold, and keeping only the first two
/// resident is what lets the tree hold more than fits in memory.
#[derive(Debug)]
pub struct SsTable {
    path: PathBuf,
    file: File,
    index: Vec<BlockHandle>,
    bloom: Bloom,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    entry_count: u64,
    file_size: u64,
}

impl SsTable {
    /// Opens a table and loads its index and filter.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = File::open(&path).ctx(&path)?;
        let file_size = file.metadata().ctx(&path)?.len();
        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::corrupt(
                &path,
                format!("{file_size} bytes is shorter than a footer"),
            ));
        }

        let mut footer = vec![0u8; FOOTER_SIZE];
        pread(&file, &path, file_size - FOOTER_SIZE as u64, &mut footer)?;

        let magic = u64::from_le_bytes(footer[FOOTER_SIZE - 8..].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::corrupt(
                &path,
                "bad magic; this is not an sstable or the tail was lost",
            ));
        }
        let signed = &footer[..FOOTER_SIZE - 12];
        let want = u32::from_le_bytes(
            footer[FOOTER_SIZE - 12..FOOTER_SIZE - 8]
                .try_into()
                .unwrap(),
        );
        if coding::checksum(signed) != want {
            return Err(Error::corrupt(&path, "footer checksum mismatch"));
        }

        let mut c = Cursor::new(&footer, &path);
        let bloom_offset = c.u64()?;
        let bloom_len = c.u64()?;
        let index_offset = c.u64()?;
        let index_len = c.u64()?;
        let entry_count = c.u64()?;
        let version = c.u32()?;
        if version != FORMAT_VERSION {
            return Err(Error::corrupt(
                &path,
                format!("format version {version}, this build reads {FORMAT_VERSION}"),
            ));
        }
        if bloom_offset + bloom_len > file_size || index_offset + index_len > file_size {
            return Err(Error::corrupt(&path, "footer points outside the file"));
        }

        let mut bloom_block = vec![0u8; bloom_len as usize];
        pread(&file, &path, bloom_offset, &mut bloom_block)?;
        let payload = coding::verify_trailing_checksum(&bloom_block, &path, "bloom block")?;
        if payload.len() < 4 {
            return Err(Error::corrupt(&path, "bloom block is too short"));
        }
        let hashes = u32::from_le_bytes(payload[..4].try_into().unwrap());
        let bloom = Bloom::from_parts(payload[4..].to_vec(), hashes);

        let mut index_block = vec![0u8; index_len as usize];
        pread(&file, &path, index_offset, &mut index_block)?;
        let payload = coding::verify_trailing_checksum(&index_block, &path, "index block")?;
        let mut c = Cursor::new(payload, &path);
        let min_key = c.bytes()?.to_vec();
        let max_key = c.bytes()?.to_vec();
        let count = c.uvarint()? as usize;
        if count > file_size as usize {
            return Err(Error::corrupt(
                &path,
                format!("index claims {count} blocks"),
            ));
        }
        let mut index = Vec::with_capacity(count);
        for _ in 0..count {
            let last_key = c.bytes()?.to_vec();
            let offset = c.uvarint()?;
            let length = c.uvarint()? as u32;
            if offset + length as u64 > bloom_offset {
                return Err(Error::corrupt(&path, "index points past the data blocks"));
            }
            index.push(BlockHandle {
                last_key,
                offset,
                length,
            });
        }

        Ok(SsTable {
            path,
            file,
            index,
            bloom,
            min_key,
            max_key,
            entry_count,
            file_size,
        })
    }

    /// Where this table lives.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of entries, tombstones included.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Size on disk.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Smallest key.
    pub fn min_key(&self) -> &[u8] {
        &self.min_key
    }

    /// Largest key.
    pub fn max_key(&self) -> &[u8] {
        &self.max_key
    }

    /// Number of data blocks.
    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    /// Reports whether `key` could be present, using only in-memory state.
    ///
    /// This is the cheap gate every read passes through: a key range check
    /// costs two comparisons and the Bloom probe costs a handful of memory
    /// accesses, and together they eliminate the vast majority of files without
    /// touching the disk at all.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        key >= self.min_key.as_slice()
            && key <= self.max_key.as_slice()
            && self.bloom.may_contain(key)
    }

    /// Looks up `key`.
    ///
    /// `Ok(None)` means this file has no opinion and the search must continue
    /// into older files. `Ok(Some(Entry::Delete))` means the key was deleted
    /// here and the search must stop.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        if !self.may_contain(key) {
            return None.into_ok();
        }
        // Find the first block whose last key is at or after the target. Any
        // earlier block ends before the key and any later one begins after it,
        // so at most one block can hold it.
        let idx = self.index.partition_point(|h| h.last_key.as_slice() < key);
        let Some(handle) = self.index.get(idx) else {
            return None.into_ok();
        };
        let block = self.read_block(handle)?;
        let payload = coding::verify_trailing_checksum(&block, &self.path, "data block")?;
        let mut c = Cursor::new(&payload[..payload.len() - 4], &self.path);
        while !c.is_empty() {
            let (k, entry) = decode_entry(&mut c)?;
            match k.cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => return Ok(Some(entry)),
                // Entries ascend, so once past the key it cannot appear later.
                std::cmp::Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }

    fn read_block(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; handle.length as usize];
        pread(&self.file, &self.path, handle.offset, &mut buf)?;
        Ok(buf)
    }

    /// Iterates every entry in key order.
    ///
    /// Compaction is the only caller, and it walks whole files sequentially,
    /// so the iterator reads one block at a time rather than mapping the file.
    pub fn iter(&self) -> SsTableIter<'_> {
        SsTableIter {
            table: self,
            block_idx: 0,
            block: Vec::new(),
            pos: 0,
            done: false,
        }
    }
}

/// A sequential reader over one SSTable.
pub struct SsTableIter<'a> {
    table: &'a SsTable,
    block_idx: usize,
    block: Vec<u8>,
    pos: usize,
    done: bool,
}

impl SsTableIter<'_> {
    /// Returns the next entry, or `None` at the end.
    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Entry)>> {
        loop {
            if self.done {
                return Ok(None);
            }
            if self.pos >= self.block.len() {
                if self.block_idx >= self.table.index.len() {
                    self.done = true;
                    return Ok(None);
                }
                let handle = &self.table.index[self.block_idx];
                self.block_idx += 1;
                let raw = self.table.read_block(handle)?;
                let payload =
                    coding::verify_trailing_checksum(&raw, &self.table.path, "data block")?;
                // Trim the trailing entry count; the loop consumes entries
                // until the payload runs out.
                self.block = payload[..payload.len() - 4].to_vec();
                self.pos = 0;
                continue;
            }
            let mut c = Cursor::new(&self.block[self.pos..], &self.table.path);
            let (k, entry) = decode_entry(&mut c)?;
            let key = k.to_vec();
            self.pos += c.position();
            return Ok(Some((key, entry)));
        }
    }
}

/// Reads exactly `buf.len()` bytes at `offset`.
///
/// Positioned reads rather than seek-then-read, because seek mutates shared
/// file state and would force a lock around every block read to keep concurrent
/// readers from stepping on each other.
fn pread(file: &File, path: &Path, offset: u64, buf: &mut [u8]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset).ctx(path)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut read = 0;
        while read < buf.len() {
            let n = file
                .seek_read(&mut buf[read..], offset + read as u64)
                .ctx(path)?;
            if n == 0 {
                return Err(Error::corrupt(
                    path,
                    "unexpected end of file during a positioned read",
                ));
            }
            read += n;
        }
        Ok(())
    }
}

/// Small helper so a `None` can be returned from a `Result` chain without a
/// turbofish at every call site.
trait IntoOk<T> {
    fn into_ok(self) -> Result<T>;
}

impl<T> IntoOk<T> for T {
    fn into_ok(self) -> Result<T> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn put(v: &str) -> Entry {
        Entry::Put(Record::new(v.as_bytes().to_vec(), 0))
    }

    fn write_table(dir: &TempDir, name: &str, pairs: &[(&str, Entry)]) -> (PathBuf, TableMeta) {
        let path = dir.path().join(name);
        let mut w = SsTableWriter::create(&path).unwrap();
        for (k, e) in pairs {
            w.add(k.as_bytes(), e).unwrap();
        }
        (path, w.finish().unwrap())
    }

    #[test]
    fn round_trips_puts_deletes_and_expiry() {
        let dir = TempDir::new().unwrap();
        let pairs = vec![
            ("alpha", put("one")),
            ("bravo", Entry::Delete),
            (
                "charlie",
                Entry::Put(Record::new(b"three".to_vec(), 1_700_000_000_000)),
            ),
            ("delta", Entry::Put(Record::new(Vec::new(), 0))),
            (
                "echo",
                Entry::Put(Record::new(vec![0u8, 0xff, b'\r', b'\n'], -5)),
            ),
        ];
        let (path, meta) = write_table(&dir, "t.sst", &pairs);

        assert_eq!(meta.entry_count, 5);
        assert_eq!(meta.min_key, b"alpha");
        assert_eq!(meta.max_key, b"echo");

        let t = SsTable::open(&path).unwrap();
        for (k, want) in &pairs {
            assert_eq!(t.get(k.as_bytes()).unwrap().as_ref(), Some(want), "key {k}");
        }
        assert_eq!(t.get(b"zulu").unwrap(), None);
        assert_eq!(t.get(b"aaaa").unwrap(), None);
        assert_eq!(t.get(b"bravado").unwrap(), None);
    }

    #[test]
    fn spans_many_blocks_and_finds_every_key() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("big.sst");
        // A small block size forces hundreds of blocks, which is what exercises
        // the index binary search rather than a single-block shortcut.
        let mut w = SsTableWriter::create_with(&path, 512, DEFAULT_BITS_PER_KEY).unwrap();
        let n = 5_000;
        for i in 0..n {
            let k = format!("key:{i:06}");
            let v = format!("value-for-{i}");
            w.add(
                k.as_bytes(),
                &Entry::Put(Record::new(v.into_bytes(), i as i64)),
            )
            .unwrap();
        }
        let meta = w.finish().unwrap();
        assert_eq!(meta.entry_count, n as u64);

        let t = SsTable::open(&path).unwrap();
        assert!(
            t.block_count() > 100,
            "expected many blocks, got {}",
            t.block_count()
        );
        for i in (0..n).step_by(7) {
            let k = format!("key:{i:06}");
            match t.get(k.as_bytes()).unwrap() {
                Some(Entry::Put(r)) => {
                    assert_eq!(r.value.as_ref(), format!("value-for-{i}").as_bytes());
                    assert_eq!(r.expire_at, i as i64);
                }
                other => panic!("key {k} returned {other:?}"),
            }
        }
        assert_eq!(t.get(b"key:999999").unwrap(), None);
    }

    #[test]
    fn iteration_returns_every_entry_in_order() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("iter.sst");
        let mut w = SsTableWriter::create_with(&path, 512, DEFAULT_BITS_PER_KEY).unwrap();
        let n = 2_000;
        for i in 0..n {
            let k = format!("k{i:05}");
            if i % 5 == 0 {
                w.add(k.as_bytes(), &Entry::Delete).unwrap();
            } else {
                w.add(k.as_bytes(), &put("v")).unwrap();
            }
        }
        w.finish().unwrap();

        let t = SsTable::open(&path).unwrap();
        let mut it = t.iter();
        let mut seen = 0usize;
        let mut prev: Option<Vec<u8>> = None;
        let mut deletes = 0usize;
        while let Some((k, e)) = it.next_entry().unwrap() {
            if let Some(p) = &prev {
                assert!(
                    &k > p,
                    "iteration went backwards at {:?}",
                    String::from_utf8_lossy(&k)
                );
            }
            if e.is_delete() {
                deletes += 1;
            }
            prev = Some(k);
            seen += 1;
        }
        assert_eq!(seen, n);
        assert_eq!(deletes, n / 5);
    }

    #[test]
    fn unsorted_keys_are_rejected_rather_than_silently_accepted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bad.sst");
        let mut w = SsTableWriter::create(&path).unwrap();
        w.add(b"b", &put("1")).unwrap();
        let err = w.add(b"a", &put("2")).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "got {err:?}");
        // A duplicate is also a caller bug: the file would have two entries for
        // one key and lookups would return whichever came first.
        assert!(w.add(b"b", &put("3")).is_err());
    }

    #[test]
    fn an_abandoned_writer_leaves_no_file_behind() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("orphan.sst");
        {
            let mut w = SsTableWriter::create(&path).unwrap();
            w.add(b"a", &put("1")).unwrap();
            // Dropped without finish, as a failing compaction would.
        }
        assert!(!path.exists(), "a partial sstable was left on disk");
    }

    #[test]
    fn an_empty_table_opens_and_answers_nothing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.sst");
        let meta = SsTableWriter::create(&path).unwrap().finish().unwrap();
        assert_eq!(meta.entry_count, 0);

        let t = SsTable::open(&path).unwrap();
        assert_eq!(t.get(b"anything").unwrap(), None);
        assert_eq!(t.iter().next_entry().unwrap(), None);
    }

    #[test]
    fn a_flipped_byte_in_a_data_block_is_caught() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bitrot.sst");
        let mut w = SsTableWriter::create_with(&path, 512, DEFAULT_BITS_PER_KEY).unwrap();
        for i in 0..500 {
            w.add(format!("key:{i:05}").as_bytes(), &put("some value here"))
                .unwrap();
        }
        w.finish().unwrap();

        let mut bytes = std::fs::read(&path).unwrap();
        // Corrupt inside the first data block, well away from the footer.
        bytes[64] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let t = SsTable::open(&path).unwrap();
        let err = t.get(b"key:00000").unwrap_err();
        assert!(
            err.is_corruption(),
            "silent bad read instead of a corruption error: {err:?}"
        );
    }

    #[test]
    fn a_truncated_file_is_rejected_at_open() {
        let dir = TempDir::new().unwrap();
        let (path, _) = write_table(&dir, "cut.sst", &[("a", put("1")), ("b", put("2"))]);
        let full = std::fs::read(&path).unwrap();

        for cut in [1usize, 10, FOOTER_SIZE, full.len() - 1] {
            std::fs::write(&path, &full[..full.len().saturating_sub(cut)]).unwrap();
            assert!(
                SsTable::open(&path).is_err(),
                "a file missing its last {cut} bytes was accepted"
            );
        }
    }

    #[test]
    fn a_corrupt_footer_is_rejected_at_open() {
        let dir = TempDir::new().unwrap();
        let (path, _) = write_table(&dir, "footer.sst", &[("a", put("1"))]);
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        // Damage an offset inside the footer while leaving the magic intact,
        // so only the footer's own checksum can catch it.
        bytes[n - 20] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        let err = SsTable::open(&path).unwrap_err();
        assert!(err.is_corruption(), "got {err:?}");
    }

    #[test]
    fn the_bloom_filter_actually_skips_absent_keys() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bloom.sst");
        let mut w = SsTableWriter::create_with(&path, 4096, DEFAULT_BITS_PER_KEY).unwrap();
        for i in 0..10_000 {
            w.add(format!("present:{i:06}").as_bytes(), &put("v"))
                .unwrap();
        }
        w.finish().unwrap();

        let t = SsTable::open(&path).unwrap();
        // Absent keys inside the file's range must mostly be rejected by the
        // filter without a block read. If may_contain were always true the
        // filter would be doing nothing.
        let mut admitted = 0;
        for i in 0..5_000 {
            if t.may_contain(format!("present:0{i:05}x").as_bytes()) {
                admitted += 1;
            }
        }
        assert!(
            admitted < 250,
            "{admitted} of 5000 absent keys got past the filter"
        );
        for i in 0..1_000 {
            assert!(
                t.may_contain(format!("present:{i:06}").as_bytes()),
                "filter rejected a key that is present"
            );
        }
    }

    #[test]
    fn key_range_metadata_answers_overlap_questions() {
        let m = TableMeta {
            file_number: 1,
            file_size: 0,
            entry_count: 0,
            min_key: b"m".to_vec(),
            max_key: b"t".to_vec(),
        };
        assert!(m.overlaps(b"a", b"z"));
        assert!(m.overlaps(b"m", b"m"));
        assert!(m.overlaps(b"t", b"z"));
        assert!(!m.overlaps(b"a", b"l"));
        assert!(!m.overlaps(b"u", b"z"));
        assert!(m.may_contain(b"p"));
        assert!(!m.may_contain(b"a"));
    }

    #[test]
    fn handles_binary_keys_and_large_values() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("binary.sst");
        let mut w = SsTableWriter::create_with(&path, 1024, DEFAULT_BITS_PER_KEY).unwrap();
        let big = vec![0xABu8; 100_000];
        // A value far larger than the block size must still be writable and
        // readable: the writer cuts a block after the entry, not before it.
        w.add(&[0u8, 1], &put("a")).unwrap();
        w.add(&[0u8, 2], &Entry::Put(Record::new(big.clone(), 0)))
            .unwrap();
        w.add(&[0xffu8; 200], &put("z")).unwrap();
        w.finish().unwrap();

        let t = SsTable::open(&path).unwrap();
        match t.get(&[0u8, 2]).unwrap() {
            Some(Entry::Put(r)) => assert_eq!(r.value.as_ref(), big.as_slice()),
            other => panic!("{other:?}"),
        }
        assert!(t.get(&[0xffu8; 200]).unwrap().is_some());
    }
}
