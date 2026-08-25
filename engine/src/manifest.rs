//! The manifest: which SSTables exist and which level each one is on.
//!
//! An SSTable on disk is invisible until the manifest names it, and stops
//! existing the moment the manifest stops naming it. That indirection is what
//! makes a flush or a compaction atomic: the new files are written and fsynced
//! first, the manifest is replaced in one atomic rename, and only then are the
//! superseded files deleted. A crash at any point leaves a manifest that names
//! a complete, consistent set of files, with at worst some orphans on disk that
//! startup sweeps away.
//!
//! The whole level layout is rewritten on every change rather than appending
//! incremental edits the way LevelDB does. Edits are the better design at
//! scale, because rewriting a manifest describing ten thousand files on every
//! compaction is wasteful. At the file counts this engine targets the rewrite
//! is a few kilobytes and buys a format with no replay logic in it at all,
//! which is one fewer place for a recovery bug to hide.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::coding::{self, Cursor, put_bytes, put_uvarint};
use crate::error::{Error, IoContext, Result};
use crate::sstable::TableMeta;
use crate::wal::sync_dir;

const MAGIC: &[u8; 8] = b"RFSMAN\x01\x00";

/// Bumped when the layout changes incompatibly.
pub const FORMAT_VERSION: u32 = 1;

/// The manifest's filename inside the data directory.
pub const MANIFEST_NAME: &str = "MANIFEST";

/// How many levels the tree has.
///
/// Seven is LevelDB's choice and it is not arbitrary: with a ten-times size
/// ratio between levels, seven levels hold a million times the size of L1,
/// which is past any realistic dataset for a single node. More levels would
/// mean more files to consult on a read that misses everything.
pub const MAX_LEVELS: usize = 7;

/// One SSTable as the manifest sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    /// The file number, which is also its name.
    pub number: u64,
    /// Size on disk.
    pub size: u64,
    /// Entries inside, tombstones included.
    pub entries: u64,
    /// Smallest key.
    pub min_key: Vec<u8>,
    /// Largest key.
    pub max_key: Vec<u8>,
}

impl FileMeta {
    /// Builds a manifest entry from a finished table.
    pub fn from_table(number: u64, m: &TableMeta) -> FileMeta {
        FileMeta {
            number,
            size: m.file_size,
            entries: m.entry_count,
            min_key: m.min_key.clone(),
            max_key: m.max_key.clone(),
        }
    }

    /// Reports whether `key` falls inside this file's range.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.min_key.as_slice() <= key && key <= self.max_key.as_slice()
    }

    /// Reports whether this file's range overlaps `[from, to]` inclusive.
    pub fn overlaps(&self, from: &[u8], to: &[u8]) -> bool {
        self.min_key.as_slice() <= to && from <= self.max_key.as_slice()
    }

    /// The file's name inside the data directory.
    pub fn file_name(&self) -> String {
        format!("{:012}.sst", self.number)
    }

    /// The file's path inside `dir`.
    pub fn path(&self, dir: &Path) -> PathBuf {
        dir.join(self.file_name())
    }
}

/// The complete set of live SSTables, by level.
///
/// Level 0 holds files flushed straight from memtables, so their key ranges
/// overlap and a read must consult all of them, newest first. Levels 1 and
/// below are maintained non-overlapping by compaction, so a read consults at
/// most one file per level and can find it by binary search.
#[derive(Debug, Clone, Default)]
pub struct Version {
    levels: Vec<Vec<Arc<FileMeta>>>,
}

impl Version {
    /// Returns an empty tree.
    pub fn new() -> Version {
        Version {
            levels: vec![Vec::new(); MAX_LEVELS],
        }
    }

    /// Files on `level`, or an empty slice.
    ///
    /// Level 0 is ordered newest first, which is the order a read must consult
    /// it in. Deeper levels are ordered by key.
    pub fn level(&self, level: usize) -> &[Arc<FileMeta>] {
        self.levels.get(level).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Number of levels.
    pub fn level_count(&self) -> usize {
        self.levels.len()
    }

    /// Total files across every level.
    pub fn file_count(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    /// Total bytes across every level.
    pub fn total_bytes(&self) -> u64 {
        self.levels.iter().flatten().map(|f| f.size).sum()
    }

    /// Bytes on one level.
    pub fn level_bytes(&self, level: usize) -> u64 {
        self.level(level).iter().map(|f| f.size).sum()
    }

    /// Total entries across every level, tombstones included.
    pub fn total_entries(&self) -> u64 {
        self.levels.iter().flatten().map(|f| f.entries).sum()
    }

    /// Every file number the version references.
    pub fn file_numbers(&self) -> Vec<u64> {
        self.levels.iter().flatten().map(|f| f.number).collect()
    }

    /// Adds a file to `level`, restoring the level's ordering invariant.
    pub fn add_file(&mut self, level: usize, file: Arc<FileMeta>) {
        while self.levels.len() <= level {
            self.levels.push(Vec::new());
        }
        self.levels[level].push(file);
        self.sort_level(level);
    }

    /// Removes files by number from `level`.
    pub fn remove_files(&mut self, level: usize, numbers: &[u64]) {
        if let Some(files) = self.levels.get_mut(level) {
            files.retain(|f| !numbers.contains(&f.number));
        }
    }

    fn sort_level(&mut self, level: usize) {
        let files = &mut self.levels[level];
        if level == 0 {
            // Newest first. Within L0 the only thing that establishes which of
            // two overlapping files holds the newer value for a key is the file
            // number, so the read path depends on this ordering for
            // correctness, not just for speed.
            files.sort_by(|a, b| b.number.cmp(&a.number));
        } else {
            files.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        }
    }

    /// Finds the file on `level` that could hold `key`.
    ///
    /// Levels below zero are non-overlapping, so binary search finds the single
    /// candidate. Level zero has no such structure and callers must scan it.
    pub fn find_in_level(&self, level: usize, key: &[u8]) -> Option<&Arc<FileMeta>> {
        debug_assert!(
            level > 0,
            "level 0 overlaps and must be scanned, not searched"
        );
        let files = self.level(level);
        let idx = files.partition_point(|f| f.max_key.as_slice() < key);
        files.get(idx).filter(|f| f.may_contain(key))
    }

    /// Files on `level` whose range overlaps `[from, to]`.
    pub fn overlapping(&self, level: usize, from: &[u8], to: &[u8]) -> Vec<Arc<FileMeta>> {
        self.level(level)
            .iter()
            .filter(|f| f.overlaps(from, to))
            .cloned()
            .collect()
    }

    /// Validates the invariants a version must satisfy.
    ///
    /// Called after loading a manifest. A tree whose deep levels overlap would
    /// return whichever value the read path happened to find first, which is a
    /// silent wrong answer rather than a crash, so it is worth one pass at
    /// startup to rule out.
    pub fn validate(&self, path: &Path) -> Result<()> {
        for (level, files) in self.levels.iter().enumerate() {
            for f in files {
                if f.min_key > f.max_key {
                    return Err(Error::corrupt(
                        path,
                        format!("file {} has min_key above max_key", f.number),
                    ));
                }
            }
            if level == 0 {
                continue;
            }
            for pair in files.windows(2) {
                if pair[0].max_key >= pair[1].min_key {
                    return Err(Error::corrupt(
                        path,
                        format!(
                            "level {level} files {} and {} overlap, which breaks the read path",
                            pair[0].number, pair[1].number
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// The durable state of the engine outside its data files.
#[derive(Debug, Clone)]
pub struct Manifest {
    /// The next unused file number.
    pub next_file_number: u64,
    /// The write-ahead log currently backing the active memtable. Logs numbered
    /// below this have been flushed into SSTables and can be deleted.
    pub log_number: u64,
    /// The highest write sequence recorded.
    pub last_sequence: u64,
    /// The level layout.
    pub version: Version,
}

impl Default for Manifest {
    fn default() -> Self {
        Manifest {
            // Zero is reserved so that "no log yet" is unambiguous.
            next_file_number: 1,
            log_number: 0,
            last_sequence: 0,
            version: Version::new(),
        }
    }
}

impl Manifest {
    /// Reserves and returns the next file number.
    pub fn allocate_file_number(&mut self) -> u64 {
        let n = self.next_file_number;
        self.next_file_number += 1;
        n
    }

    /// Reads the manifest from `dir`, returning the default if there is none.
    pub fn load(dir: &Path) -> Result<Manifest> {
        let path = dir.join(MANIFEST_NAME);
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Manifest::default()),
            Err(e) => return Err(Error::io(&path, e)),
        };
        if data.len() < 12 {
            return Err(Error::corrupt(&path, "shorter than a header"));
        }
        if &data[..8] != MAGIC {
            return Err(Error::corrupt(&path, "bad magic"));
        }
        let payload = coding::verify_trailing_checksum(&data, &path, "manifest")?;

        let mut c = Cursor::new(&payload[8..], &path);
        let version = c.u32()?;
        if version != FORMAT_VERSION {
            return Err(Error::corrupt(
                &path,
                format!("format version {version}, this build reads {FORMAT_VERSION}"),
            ));
        }
        let next_file_number = c.uvarint()?;
        let log_number = c.uvarint()?;
        let last_sequence = c.uvarint()?;
        let level_count = c.uvarint()? as usize;
        if level_count > 64 {
            return Err(Error::corrupt(
                &path,
                format!("claims {level_count} levels"),
            ));
        }

        let mut v = Version {
            levels: vec![Vec::new(); level_count.max(MAX_LEVELS)],
        };
        for level in 0..level_count {
            let n = c.uvarint()? as usize;
            if n > payload.len() {
                return Err(Error::corrupt(
                    &path,
                    format!("level {level} claims {n} files"),
                ));
            }
            for _ in 0..n {
                let number = c.uvarint()?;
                let size = c.uvarint()?;
                let entries = c.uvarint()?;
                let min_key = c.bytes()?.to_vec();
                let max_key = c.bytes()?.to_vec();
                v.levels[level].push(Arc::new(FileMeta {
                    number,
                    size,
                    entries,
                    min_key,
                    max_key,
                }));
            }
            v.sort_level(level);
        }
        if c.remaining() != 0 {
            return Err(Error::corrupt(
                &path,
                format!("{} trailing bytes", c.remaining()),
            ));
        }
        v.validate(&path)?;

        Ok(Manifest {
            next_file_number,
            log_number,
            last_sequence,
            version: v,
        })
    }

    /// Writes the manifest to `dir` atomically.
    ///
    /// Temporary file, fsync, rename, fsync the directory. A crash at any point
    /// leaves either the old manifest or the new one, never a half-written file
    /// wearing the real name. The directory fsync is not optional: without it
    /// the rename can be lost on a power cut even though the file's contents
    /// were already durable.
    pub fn store(&self, dir: &Path) -> Result<()> {
        let mut buf = Vec::with_capacity(1024);
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        put_uvarint(&mut buf, self.next_file_number);
        put_uvarint(&mut buf, self.log_number);
        put_uvarint(&mut buf, self.last_sequence);
        put_uvarint(&mut buf, self.version.levels.len() as u64);
        for files in &self.version.levels {
            put_uvarint(&mut buf, files.len() as u64);
            for f in files {
                put_uvarint(&mut buf, f.number);
                put_uvarint(&mut buf, f.size);
                put_uvarint(&mut buf, f.entries);
                put_bytes(&mut buf, &f.min_key);
                put_bytes(&mut buf, &f.max_key);
            }
        }
        let sum = coding::checksum(&buf);
        buf.extend_from_slice(&sum.to_le_bytes());

        let final_path = dir.join(MANIFEST_NAME);
        let tmp = dir.join(format!("{MANIFEST_NAME}.tmp.{}", std::process::id()));
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp).ctx(&tmp)?;
            if let Err(e) = f
                .write_all(&buf)
                .ctx(&tmp)
                .and_then(|()| f.sync_all().ctx(&tmp))
            {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        }
        if let Err(e) = std::fs::rename(&tmp, &final_path).ctx(&final_path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        sync_dir(dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn meta(number: u64, min: &str, max: &str) -> Arc<FileMeta> {
        Arc::new(FileMeta {
            number,
            size: 1000 + number,
            entries: 10 + number,
            min_key: min.as_bytes().to_vec(),
            max_key: max.as_bytes().to_vec(),
        })
    }

    #[test]
    fn a_missing_manifest_is_a_fresh_database() {
        let dir = TempDir::new().unwrap();
        let m = Manifest::load(dir.path()).unwrap();
        assert_eq!(m.next_file_number, 1);
        assert_eq!(m.version.file_count(), 0);
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let mut m = Manifest {
            next_file_number: 42,
            log_number: 7,
            last_sequence: 12345,
            ..Manifest::default()
        };
        m.version.add_file(0, meta(3, "a", "z"));
        m.version.add_file(0, meta(5, "b", "y"));
        m.version.add_file(1, meta(1, "a", "m"));
        m.version.add_file(1, meta(2, "n", "z"));
        m.version.add_file(
            3,
            Arc::new(FileMeta {
                number: 9,
                size: 1009,
                entries: 19,
                // Binary keys must survive the round trip: keys are arbitrary
                // bytes, not text, and a manifest that assumed UTF-8 would corrupt
                // any key holding a high byte.
                min_key: vec![0x00],
                max_key: vec![0xff, 0xfe],
            }),
        );
        m.store(dir.path()).unwrap();

        let got = Manifest::load(dir.path()).unwrap();
        assert_eq!(got.next_file_number, 42);
        assert_eq!(got.log_number, 7);
        assert_eq!(got.last_sequence, 12345);
        assert_eq!(got.version.file_count(), 5);
        assert_eq!(got.version.level(0).len(), 2);
        assert_eq!(got.version.level(1).len(), 2);
        assert_eq!(got.version.level(3).len(), 1);
        assert_eq!(
            got.version.level(0)[0].number,
            5,
            "level 0 must be newest first"
        );
        assert_eq!(
            got.version.level(1)[0].number,
            1,
            "deep levels must be sorted by key"
        );
        assert_eq!(got.version.total_bytes(), m.version.total_bytes());
    }

    #[test]
    fn replacing_the_manifest_is_atomic() {
        let dir = TempDir::new().unwrap();
        let mut m = Manifest::default();
        m.version.add_file(1, meta(1, "a", "m"));
        m.store(dir.path()).unwrap();

        let mut m2 = Manifest::load(dir.path()).unwrap();
        m2.version.add_file(1, meta(2, "n", "z"));
        m2.last_sequence = 99;
        m2.store(dir.path()).unwrap();

        let got = Manifest::load(dir.path()).unwrap();
        assert_eq!(got.version.level(1).len(), 2);
        assert_eq!(got.last_sequence, 99);

        // No temporary files may be left behind.
        let strays: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n != MANIFEST_NAME)
            .collect();
        assert!(strays.is_empty(), "left behind {strays:?}");
    }

    #[test]
    fn corruption_is_rejected_rather_than_partially_loaded() {
        let dir = TempDir::new().unwrap();
        let mut m = Manifest::default();
        for i in 1..10 {
            m.version
                .add_file(1, meta(i, &format!("k{i:02}a"), &format!("k{i:02}z")));
        }
        m.store(dir.path()).unwrap();

        let path = dir.path().join(MANIFEST_NAME);
        let full = std::fs::read(&path).unwrap();

        // A flipped byte anywhere must fail the checksum.
        for pos in [9usize, full.len() / 2, full.len() - 6] {
            let mut bad = full.clone();
            bad[pos] ^= 0xff;
            std::fs::write(&path, &bad).unwrap();
            assert!(
                Manifest::load(dir.path()).unwrap_err().is_corruption(),
                "byte {pos} was accepted"
            );
        }
        // A truncated manifest must also fail.
        std::fs::write(&path, &full[..full.len() - 4]).unwrap();
        assert!(Manifest::load(dir.path()).is_err());
    }

    #[test]
    fn overlapping_deep_levels_are_refused_at_load() {
        // A tree whose deep levels overlap returns whichever value the read
        // path finds first: a silent wrong answer, which is worse than a
        // refusal to start.
        let dir = TempDir::new().unwrap();
        let mut m = Manifest::default();
        m.version.add_file(1, meta(1, "a", "m"));
        m.version.add_file(1, meta(2, "k", "z"));
        m.store(dir.path()).unwrap();
        let err = Manifest::load(dir.path()).unwrap_err();
        assert!(err.is_corruption(), "{err:?}");
    }

    #[test]
    fn find_in_level_locates_the_only_candidate() {
        let mut v = Version::new();
        v.add_file(1, meta(1, "a", "f"));
        v.add_file(1, meta(2, "h", "m"));
        v.add_file(1, meta(3, "p", "z"));

        assert_eq!(v.find_in_level(1, b"c").unwrap().number, 1);
        assert_eq!(v.find_in_level(1, b"a").unwrap().number, 1);
        assert_eq!(v.find_in_level(1, b"f").unwrap().number, 1);
        assert_eq!(v.find_in_level(1, b"j").unwrap().number, 2);
        assert_eq!(v.find_in_level(1, b"z").unwrap().number, 3);
        // Keys in the gaps between files belong to no file.
        assert!(v.find_in_level(1, b"g").is_none());
        assert!(v.find_in_level(1, b"n").is_none());
        assert!(v.find_in_level(1, b"zz").is_none());
    }

    #[test]
    fn overlapping_selects_the_right_files() {
        let mut v = Version::new();
        v.add_file(1, meta(1, "a", "f"));
        v.add_file(1, meta(2, "h", "m"));
        v.add_file(1, meta(3, "p", "z"));

        let got: Vec<u64> = v
            .overlapping(1, b"e", b"i")
            .iter()
            .map(|f| f.number)
            .collect();
        assert_eq!(got, vec![1, 2]);
        let got: Vec<u64> = v
            .overlapping(1, b"aa", b"zz")
            .iter()
            .map(|f| f.number)
            .collect();
        assert_eq!(got, vec![1, 2, 3]);
        assert!(v.overlapping(1, b"g", b"g").is_empty());
    }

    #[test]
    fn removing_files_updates_the_level() {
        let mut v = Version::new();
        v.add_file(0, meta(1, "a", "z"));
        v.add_file(0, meta(2, "a", "z"));
        v.add_file(0, meta(3, "a", "z"));
        v.remove_files(0, &[1, 3]);
        assert_eq!(v.level(0).len(), 1);
        assert_eq!(v.level(0)[0].number, 2);
        // Removing something absent is not an error.
        v.remove_files(0, &[99]);
        assert_eq!(v.level(0).len(), 1);
    }

    #[test]
    fn file_numbers_are_allocated_monotonically() {
        let mut m = Manifest::default();
        let a = m.allocate_file_number();
        let b = m.allocate_file_number();
        assert!(
            b > a && a > 0,
            "file numbers must be positive and increasing"
        );
    }
}
