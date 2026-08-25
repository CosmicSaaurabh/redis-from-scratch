//! The units of storage shared by every layer of the engine.

use std::sync::Arc;

/// A stored value and its absolute expiry.
///
/// `expire_at` is Unix milliseconds, and zero means the record never expires.
/// Absolute rather than relative is a durability requirement: a relative TTL
/// recovered from a log hours after it was written would silently extend every
/// key's life by the length of the outage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// The value bytes.
    ///
    /// `Arc<[u8]>` rather than `Vec<u8>` because a value written once is read
    /// many times, lives in a memtable that may be shared with a background
    /// flush, and is handed out to readers. Reference counting it means none of
    /// those paths copies.
    pub value: Arc<[u8]>,
    /// Absolute expiry in Unix milliseconds, or zero for never.
    pub expire_at: i64,
}

impl Record {
    /// Builds a record from owned bytes.
    pub fn new(value: Vec<u8>, expire_at: i64) -> Self {
        Record {
            value: value.into(),
            expire_at,
        }
    }

    /// Reports whether the record is logically gone as of `now_ms`.
    pub fn expired(&self, now_ms: i64) -> bool {
        self.expire_at != 0 && self.expire_at <= now_ms
    }

    /// Approximate heap cost, used for memtable sizing.
    pub fn heap_size(&self) -> usize {
        self.value.len() + std::mem::size_of::<Record>()
    }
}

/// What a key maps to at one level of the tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    /// The key holds this record.
    Put(Record),
    /// The key was deleted.
    ///
    /// A delete has to be written down rather than simply removing the key,
    /// because older SSTables further down the tree still hold the previous
    /// value and would otherwise resurrect it. The tombstone is what shadows
    /// them, and it can only be discarded once compaction has merged past
    /// every file that could contain an older version.
    Delete,
}

impl Entry {
    /// Returns the record if this is a live, unexpired entry.
    pub fn live(&self, now_ms: i64) -> Option<&Record> {
        match self {
            Entry::Put(r) if !r.expired(now_ms) => Some(r),
            _ => None,
        }
    }

    /// Reports whether this entry is a tombstone.
    pub fn is_delete(&self) -> bool {
        matches!(self, Entry::Delete)
    }

    /// Approximate heap cost.
    pub fn heap_size(&self) -> usize {
        match self {
            Entry::Put(r) => r.heap_size(),
            Entry::Delete => std::mem::size_of::<Entry>(),
        }
    }
}

/// One mutation in a batch.
#[derive(Clone, Debug)]
pub struct Mutation {
    /// The key.
    pub key: Vec<u8>,
    /// What to do with it.
    pub entry: Entry,
}

impl Mutation {
    /// Builds a put.
    pub fn put(key: Vec<u8>, value: Vec<u8>, expire_at: i64) -> Self {
        Mutation {
            key,
            entry: Entry::Put(Record::new(value, expire_at)),
        }
    }

    /// Builds a delete.
    pub fn delete(key: Vec<u8>) -> Self {
        Mutation {
            key,
            entry: Entry::Delete,
        }
    }
}
