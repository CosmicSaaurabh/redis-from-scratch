//! The in-memory write buffer.
//!
//! Every write lands here first, after the write-ahead log. When it grows past
//! a threshold it is frozen, a fresh one takes over, and a background thread
//! writes the frozen one out as an SSTable. That is the whole reason an LSM
//! tree is fast to write: random writes are absorbed in memory and reach the
//! disk as one large sequential file.

use std::collections::BTreeMap;
use std::ops::Bound;

use crate::types::{Entry, Record};

/// A sorted in-memory table.
///
/// The backing structure is a `BTreeMap` rather than the skip list a
/// production engine would use. A skip list allows lock-free concurrent
/// readers alongside a writer; a `BTreeMap` needs an `RwLock` around it. The
/// trade is deliberate for this build: the lock is held for a single map
/// operation and never across I/O, and a correct B-tree from the standard
/// library is worth more than a hand-rolled skip list with a subtle memory
/// ordering bug in it. The seam is narrow enough to swap later.
#[derive(Debug, Default)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Entry>,
    /// Approximate heap footprint, tracked incrementally because walking the
    /// map to size it would make every write O(n).
    bytes: usize,
}

impl MemTable {
    /// Returns an empty memtable.
    pub fn new() -> Self {
        MemTable {
            map: BTreeMap::new(),
            bytes: 0,
        }
    }

    /// Inserts or replaces a key.
    pub fn insert(&mut self, key: Vec<u8>, entry: Entry) {
        let added = key.len() + entry.heap_size();
        if let Some(old) = self.map.insert(key.clone(), entry) {
            // Replacing a key does not add a key's worth of overhead again.
            self.bytes = self.bytes.saturating_sub(old.heap_size());
            self.bytes += added - key.len();
        } else {
            self.bytes += added;
        }
    }

    /// Returns the entry for `key`, if this table has an opinion about it.
    ///
    /// A returned [`Entry::Delete`] is an answer, not an absence: it means the
    /// key was deleted at this level and the search must stop rather than
    /// falling through to an older SSTable that still holds the old value.
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.map.get(key)
    }

    /// Number of keys held.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Reports whether the table holds nothing.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Approximate heap footprint in bytes.
    pub fn approx_bytes(&self) -> usize {
        self.bytes
    }

    /// Iterates keys in sorted order, which is exactly the order an SSTable
    /// needs them written in.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Entry)> {
        self.map.iter()
    }

    /// Iterates from `start` inclusive.
    pub fn range_from<'a>(
        &'a self,
        start: &'a [u8],
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a Entry)> {
        self.map
            .range::<[u8], _>((Bound::Included(start), Bound::Unbounded))
    }

    /// Returns the smallest and largest key, if any.
    pub fn key_range(&self) -> Option<(&Vec<u8>, &Vec<u8>)> {
        Some((self.map.first_key_value()?.0, self.map.last_key_value()?.0))
    }

    /// Counts live, unexpired records. Used for reporting, not on any hot path.
    pub fn live_len(&self, now_ms: i64) -> usize {
        self.map
            .values()
            .filter(|e| e.live(now_ms).is_some())
            .count()
    }

    /// Returns the live record for `key`, resolving tombstones and expiry.
    ///
    /// The outer `Option` is "this table knows"; the inner is "and the answer
    /// is a live record". Collapsing them would lose the distinction between
    /// "deleted here" and "not mentioned here", and the second must continue
    /// the search down the tree while the first must not.
    pub fn lookup(&self, key: &[u8], now_ms: i64) -> Option<Option<Record>> {
        match self.map.get(key)? {
            Entry::Delete => Some(None),
            Entry::Put(r) if r.expired(now_ms) => Some(None),
            Entry::Put(r) => Some(Some(r.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(v: &str) -> Entry {
        Entry::Put(Record::new(v.as_bytes().to_vec(), 0))
    }

    #[test]
    fn insert_and_get() {
        let mut m = MemTable::new();
        m.insert(b"a".to_vec(), put("1"));
        m.insert(b"b".to_vec(), put("2"));
        assert_eq!(m.len(), 2);
        assert_eq!(m.get(b"a"), Some(&put("1")));
        assert_eq!(m.get(b"z"), None);
    }

    #[test]
    fn replacing_a_key_does_not_grow_the_count() {
        let mut m = MemTable::new();
        m.insert(b"k".to_vec(), put("short"));
        let after_first = m.approx_bytes();
        m.insert(b"k".to_vec(), put("a much longer value than before"));
        assert_eq!(m.len(), 1);
        assert!(
            m.approx_bytes() > after_first,
            "size accounting ignored the larger value"
        );

        m.insert(b"k".to_vec(), put("s"));
        assert!(
            m.approx_bytes() < after_first + 40,
            "size accounting did not shrink: {}",
            m.approx_bytes()
        );
    }

    #[test]
    fn a_tombstone_is_an_answer_not_an_absence() {
        let mut m = MemTable::new();
        m.insert(b"k".to_vec(), put("v"));
        m.insert(b"k".to_vec(), Entry::Delete);

        // The table knows about the key, and the answer is that it is gone.
        assert_eq!(m.lookup(b"k", 0), Some(None));
        // The table has no opinion about this one, so the caller must keep
        // looking further down the tree.
        assert_eq!(m.lookup(b"other", 0), None);
    }

    #[test]
    fn expiry_is_resolved_at_lookup() {
        let mut m = MemTable::new();
        m.insert(b"k".to_vec(), Entry::Put(Record::new(b"v".to_vec(), 1_000)));
        assert!(m.lookup(b"k", 999).unwrap().is_some());
        assert_eq!(m.lookup(b"k", 1_000), Some(None));
        assert_eq!(m.lookup(b"k", 5_000), Some(None));
    }

    #[test]
    fn iteration_is_sorted() {
        let mut m = MemTable::new();
        for k in ["delta", "alpha", "charlie", "bravo"] {
            m.insert(k.as_bytes().to_vec(), put(k));
        }
        let keys: Vec<String> = m
            .iter()
            .map(|(k, _)| String::from_utf8(k.clone()).unwrap())
            .collect();
        assert_eq!(keys, vec!["alpha", "bravo", "charlie", "delta"]);
    }

    #[test]
    fn range_from_starts_at_the_right_place() {
        let mut m = MemTable::new();
        for k in ["a", "b", "c", "d"] {
            m.insert(k.as_bytes().to_vec(), put(k));
        }
        let keys: Vec<Vec<u8>> = m.range_from(b"b").map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);

        let keys: Vec<Vec<u8>> = m.range_from(b"bb").map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"c".to_vec(), b"d".to_vec()]);
    }

    #[test]
    fn key_range_reports_the_extremes() {
        let mut m = MemTable::new();
        assert!(m.key_range().is_none());
        for k in ["m", "a", "z"] {
            m.insert(k.as_bytes().to_vec(), put(k));
        }
        let (lo, hi) = m.key_range().unwrap();
        assert_eq!(lo, b"a");
        assert_eq!(hi, b"z");
    }
}
