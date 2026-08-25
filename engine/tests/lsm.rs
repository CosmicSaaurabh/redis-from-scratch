//! End-to-end tests for the LSM tree.
//!
//! These drive the whole engine rather than a single module, because the
//! properties that matter - a read finds the newest version of a key wherever
//! it lives, a restart loses nothing acknowledged, compaction does not
//! resurrect deleted data - are all properties of the layers working together.

use std::sync::Arc;

use engine::db::{Db, Options};
use engine::types::Mutation;
use engine::wal::SyncPolicy;
use tempfile::TempDir;

/// Options tuned so that a few thousand small keys exercise flushing and
/// several levels of compaction, instead of sitting in one memtable forever.
fn small(dir: &TempDir) -> Options {
    Options {
        dir: dir.path().to_path_buf(),
        memtable_bytes: 16 * 1024,
        block_size: 1024,
        // Most of these tests are about the tree's structure, not its fsync
        // discipline, and fsyncing every write makes the suite spend its time
        // in the kernel. The durability tests that genuinely need `Always`
        // override it.
        sync_policy: SyncPolicy::EverySecond,
        l0_compaction_trigger: 2,
        l0_stop_writes_trigger: 8,
        level_base_bytes: 32 * 1024,
        level_multiplier: 4,
        max_file_bytes: 16 * 1024,
        max_immutable: 2,
        ..Options::default()
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("key:{i:08}").into_bytes()
}

fn val(i: usize) -> Vec<u8> {
    format!("value-{i}-{}", "x".repeat(40)).into_bytes()
}

fn get_str(db: &Db, k: &[u8]) -> Option<String> {
    db.get(k, 0)
        .unwrap()
        .map(|r| String::from_utf8(r.value.to_vec()).unwrap())
}

#[test]
fn writes_are_readable_across_memtable_and_every_level() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    const N: usize = 4_000;
    for i in 0..N {
        db.put(key(i), val(i), 0).unwrap();
    }
    // Force the tree into a multi-level shape so the read path has to search
    // more than the memtable.
    db.flush_memtable().unwrap();
    db.compact_all().unwrap();

    let stats = db.stats(0).unwrap();
    assert!(stats.flushes > 0, "nothing was ever flushed to disk");
    assert!(stats.disk_bytes > 0, "no sstables were produced");

    for i in 0..N {
        assert_eq!(
            get_str(&db, &key(i)),
            Some(String::from_utf8(val(i)).unwrap()),
            "key {i} was lost"
        );
    }
    assert_eq!(get_str(&db, b"key:99999999"), None);
}

#[test]
fn the_newest_version_of_a_key_always_wins() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    // Write the same keys repeatedly with flushes in between, so each key ends
    // up with old versions scattered across several files at several levels.
    for round in 0..6 {
        for i in 0..300 {
            db.put(key(i), format!("round-{round}").into_bytes(), 0)
                .unwrap();
        }
        db.flush_memtable().unwrap();
    }
    db.compact_all().unwrap();

    for i in 0..300 {
        assert_eq!(
            get_str(&db, &key(i)),
            Some("round-5".to_string()),
            "key {i} returned a stale version"
        );
    }
}

#[test]
fn a_delete_shadows_every_older_version() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    for i in 0..500 {
        db.put(key(i), val(i), 0).unwrap();
    }
    db.flush_memtable().unwrap();
    for i in 0..500 {
        if i % 2 == 0 {
            db.delete(key(i)).unwrap();
        }
    }
    db.flush_memtable().unwrap();

    for i in 0..500 {
        let got = get_str(&db, &key(i));
        if i % 2 == 0 {
            assert_eq!(got, None, "deleted key {i} is still readable");
        } else {
            assert!(got.is_some(), "key {i} disappeared");
        }
    }

    // Compaction must not resurrect anything: a tombstone may only be dropped
    // once nothing below it could still hold an older value.
    db.compact_all().unwrap();
    for i in (0..500).step_by(2) {
        assert_eq!(
            get_str(&db, &key(i)),
            None,
            "compaction resurrected deleted key {i}"
        );
    }
}

#[test]
fn a_deleted_key_can_be_written_again() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    db.put(b"k".to_vec(), b"first".to_vec(), 0).unwrap();
    db.flush_memtable().unwrap();
    db.delete(b"k".to_vec()).unwrap();
    db.flush_memtable().unwrap();
    db.put(b"k".to_vec(), b"second".to_vec(), 0).unwrap();
    db.flush_memtable().unwrap();
    db.compact_all().unwrap();

    assert_eq!(get_str(&db, b"k"), Some("second".to_string()));
}

#[test]
fn expiry_is_absolute_and_survives_compaction() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    db.put(b"never".to_vec(), b"v".to_vec(), 0).unwrap();
    db.put(b"soon".to_vec(), b"v".to_vec(), 1_000).unwrap();
    db.put(b"later".to_vec(), b"v".to_vec(), 10_000).unwrap();
    db.flush_memtable().unwrap();

    assert!(db.get(b"soon", 999).unwrap().is_some());
    assert!(
        db.get(b"soon", 1_000).unwrap().is_none(),
        "an expired record was returned"
    );
    assert!(db.get(b"later", 1_000).unwrap().is_some());
    assert!(
        db.get(b"never", i64::MAX - 1).unwrap().is_some(),
        "a record with no TTL expired"
    );
}

#[test]
fn everything_acknowledged_survives_a_restart() {
    let dir = TempDir::new().unwrap();
    const N: usize = 3_000;
    {
        let db = Db::open(small(&dir)).unwrap();
        for i in 0..N {
            db.put(key(i), val(i), 0).unwrap();
        }
        for i in (0..N).step_by(3) {
            db.delete(key(i)).unwrap();
        }
        db.close().unwrap();
    }

    let db = Db::open(small(&dir)).unwrap();
    for i in 0..N {
        let got = get_str(&db, &key(i));
        if i % 3 == 0 {
            assert_eq!(got, None, "key {i} came back after being deleted");
        } else {
            assert_eq!(
                got,
                Some(String::from_utf8(val(i)).unwrap()),
                "key {i} was lost across the restart"
            );
        }
    }
}

#[test]
fn an_abandoned_engine_recovers_from_its_log() {
    // Nothing is closed, standing in for a process that was killed. Under the
    // always policy every acknowledged write is already fsynced, so all of them
    // must come back.
    let dir = TempDir::new().unwrap();
    const N: usize = 800;
    {
        // Nothing gets a chance to flush on the way out, so this is the one
        // test that depends on every acknowledged write already being fsynced.
        let opts = Options {
            sync_policy: SyncPolicy::Always,
            ..small(&dir)
        };
        let db = Db::open(opts).unwrap();
        for i in 0..N {
            db.put(key(i), val(i), 0).unwrap();
        }
        std::mem::forget(db);
    }

    let db = Db::open(small(&dir)).unwrap();
    for i in 0..N {
        assert_eq!(
            get_str(&db, &key(i)),
            Some(String::from_utf8(val(i)).unwrap()),
            "key {i} lost"
        );
    }
}

#[test]
fn compaction_reduces_level_zero_and_keeps_every_key() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    const N: usize = 6_000;
    for i in 0..N {
        db.put(key(i), val(i), 0).unwrap();
    }
    db.flush_memtable().unwrap();
    db.compact_all().unwrap();

    let stats = db.stats(0).unwrap();
    assert!(stats.compactions > 0, "no compaction ran");
    assert!(
        stats.level_files[0] <= 2,
        "level 0 still holds {} files after compaction",
        stats.level_files[0]
    );
    let deeper: usize = stats.level_files[1..].iter().sum();
    assert!(deeper > 0, "compaction produced no files below level 0");

    for i in (0..N).step_by(11) {
        assert!(
            get_str(&db, &key(i)).is_some(),
            "key {i} lost during compaction"
        );
    }
}

#[test]
fn space_amplification_stays_bounded_under_repeated_overwrites() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    // Rewriting the same small key set many times creates a lot of dead
    // versions. Collapsing them is the entire job of compaction, and the
    // property that matters is that the disk footprint tracks the live data
    // rather than the total ever written.
    const KEYS: usize = 400;
    const ROUNDS: usize = 12;
    for _ in 0..ROUNDS {
        for i in 0..KEYS {
            db.put(key(i), val(i), 0).unwrap();
        }
        db.flush_memtable().unwrap();
    }
    db.compact_all().unwrap();
    let stats = db.stats(0).unwrap();

    let live_bytes: u64 = (0..KEYS)
        .map(|i| (key(i).len() + val(i).len()) as u64)
        .sum();
    let written_bytes = live_bytes * ROUNDS as u64;
    assert!(
        stats.disk_bytes < live_bytes * 4,
        "space amplification is {:.1}x live data ({} bytes on disk for {live_bytes} bytes of data)",
        stats.disk_bytes as f64 / live_bytes as f64,
        stats.disk_bytes
    );
    assert!(
        stats.disk_bytes < written_bytes / 2,
        "nothing was collapsed: {} bytes on disk after writing {written_bytes}",
        stats.disk_bytes
    );
    for i in 0..KEYS {
        assert!(get_str(&db, &key(i)).is_some(), "key {i} lost");
    }
}

#[test]
fn scan_returns_every_live_key_in_order() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    const N: usize = 2_000;
    for i in 0..N {
        db.put(key(i), val(i), 0).unwrap();
    }
    for i in (0..N).step_by(4) {
        db.delete(key(i)).unwrap();
    }
    db.flush_memtable().unwrap();
    db.compact_all().unwrap();

    let mut seen = Vec::new();
    let mut start = Vec::new();
    loop {
        let (page, next) = db.scan(&start, 100, 0).unwrap();
        for (k, _) in &page {
            seen.push(k.clone());
        }
        match next {
            Some(k) => start = k,
            None => break,
        }
    }

    let expected: Vec<Vec<u8>> = (0..N).filter(|i| i % 4 != 0).map(key).collect();
    assert_eq!(
        seen.len(),
        expected.len(),
        "scan returned the wrong number of keys"
    );
    assert_eq!(seen, expected, "scan did not return keys in sorted order");
}

#[test]
fn scan_sees_the_newest_version_of_a_key() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    for i in 0..200 {
        db.put(key(i), b"old".to_vec(), 0).unwrap();
    }
    db.flush_memtable().unwrap();
    for i in 0..200 {
        db.put(key(i), b"new".to_vec(), 0).unwrap();
    }

    let (page, _) = db.scan(b"", 500, 0).unwrap();
    assert_eq!(page.len(), 200);
    for (k, r) in page {
        assert_eq!(
            r.value.as_ref(),
            b"new",
            "scan returned a stale version of {:?}",
            String::from_utf8_lossy(&k)
        );
    }
}

#[test]
fn concurrent_writers_and_readers_stay_consistent() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Db::open(small(&dir)).unwrap());

    const THREADS: usize = 8;
    const EACH: usize = 500;
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for i in 0..EACH {
                let k = format!("t{t}:{i:05}").into_bytes();
                db.put(k.clone(), format!("v{t}-{i}").into_bytes(), 0)
                    .unwrap();
                // Read back immediately: a write that has been acknowledged
                // must be visible to the next read on any thread.
                let got = db.get(&k, 0).unwrap().expect("own write not visible");
                assert_eq!(got.value.as_ref(), format!("v{t}-{i}").as_bytes());
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    for t in 0..THREADS {
        for i in 0..EACH {
            let k = format!("t{t}:{i:05}").into_bytes();
            let got = db.get(&k, 0).unwrap().expect("key lost under concurrency");
            assert_eq!(got.value.as_ref(), format!("v{t}-{i}").as_bytes());
        }
    }
}

#[test]
fn a_batch_is_all_or_nothing_across_a_restart() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::open(small(&dir)).unwrap();
        db.write(vec![
            Mutation::put(b"a".to_vec(), b"1".to_vec(), 0),
            Mutation::put(b"b".to_vec(), b"2".to_vec(), 0),
            Mutation::put(b"c".to_vec(), b"3".to_vec(), 0),
        ])
        .unwrap();
        db.close().unwrap();
    }
    let db = Db::open(small(&dir)).unwrap();
    for (k, v) in [(&b"a"[..], "1"), (b"b", "2"), (b"c", "3")] {
        assert_eq!(get_str(&db, k), Some(v.to_string()));
    }
}

#[test]
fn orphaned_sstables_are_swept_at_startup() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::open(small(&dir)).unwrap();
        for i in 0..500 {
            db.put(key(i), val(i), 0).unwrap();
        }
        db.flush_memtable().unwrap();
        db.close().unwrap();
    }
    // Debris from a flush that died before its manifest write. It is
    // unreachable by construction, so startup must remove it rather than let it
    // accumulate on every crash.
    let orphan = dir.path().join("000000999999.sst");
    std::fs::write(&orphan, b"not a real sstable").unwrap();

    let db = Db::open(small(&dir)).unwrap();
    assert!(
        db.recovery().orphans_removed >= 1,
        "the orphan was not swept"
    );
    assert!(!orphan.exists());
    for i in 0..500 {
        assert!(get_str(&db, &key(i)).is_some(), "key {i} lost");
    }
}

#[test]
fn a_torn_log_tail_costs_only_the_unacknowledged_write() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::open(small(&dir)).unwrap();
        for i in 0..100 {
            db.put(key(i), val(i), 0).unwrap();
        }
        db.sync().unwrap();
        std::mem::forget(db);
    }

    // Chop the end of the newest log, exactly as an interrupted write would.
    let mut logs: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| {
            let e = e.unwrap();
            let n = e.file_name().to_string_lossy().to_string();
            n.ends_with(".log").then_some(e.path())
        })
        .collect();
    logs.sort();
    let last = logs.last().unwrap();
    let bytes = std::fs::read(last).unwrap();
    std::fs::write(last, &bytes[..bytes.len() - 9]).unwrap();

    let db = Db::open(small(&dir)).unwrap();
    assert!(db.recovery().truncated, "the torn tail was not detected");
    // Everything before the torn record must still be there.
    for i in 0..98 {
        assert!(
            get_str(&db, &key(i)).is_some(),
            "key {i} was lost to a torn tail at the end"
        );
    }
}

#[test]
fn an_empty_database_behaves() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();
    assert_eq!(db.get(b"missing", 0).unwrap(), None);
    assert_eq!(db.exact_len(0).unwrap(), 0);
    let (page, next) = db.scan(b"", 10, 0).unwrap();
    assert!(page.is_empty() && next.is_none());
    db.flush_memtable().unwrap();
    db.compact_all().unwrap();
    db.close().unwrap();
}

#[test]
fn binary_keys_and_large_values_round_trip() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    let big = vec![0xABu8; 200_000];
    db.put(vec![0x00, 0xff, b'\r', b'\n'], big.clone(), 0)
        .unwrap();
    db.put(vec![0xff; 100], b"edge".to_vec(), 0).unwrap();
    db.put(Vec::new(), b"empty key".to_vec(), 0).unwrap();
    db.flush_memtable().unwrap();
    db.compact_all().unwrap();

    assert_eq!(
        db.get(&[0x00, 0xff, b'\r', b'\n'], 0)
            .unwrap()
            .unwrap()
            .value
            .as_ref(),
        big.as_slice()
    );
    assert_eq!(
        db.get(&[0xffu8; 100], 0).unwrap().unwrap().value.as_ref(),
        b"edge"
    );
    assert_eq!(
        db.get(b"", 0).unwrap().unwrap().value.as_ref(),
        b"empty key"
    );
}

#[test]
fn level_zero_backpressure_stalls_writers_instead_of_growing_without_bound() {
    let dir = TempDir::new().unwrap();
    let mut opts = small(&dir);
    // Make flushing trivially easy to trigger and compaction hard to keep up
    // with, which is exactly the condition backpressure exists for.
    opts.memtable_bytes = 4 * 1024;
    opts.l0_stop_writes_trigger = 4;
    let db = Db::open(opts).unwrap();

    for i in 0..8_000 {
        db.put(key(i), val(i), 0).unwrap();
    }
    let stats = db.stats(0).unwrap();
    assert!(
        stats.level_files[0] <= 12,
        "level 0 grew to {} files: backpressure did not hold",
        stats.level_files[0]
    );
    // Whatever the stalling did, no write may have been lost.
    for i in (0..8_000).step_by(97) {
        assert!(
            get_str(&db, &key(i)).is_some(),
            "key {i} lost under backpressure"
        );
    }
}

#[test]
fn bloom_filters_actually_prevent_block_reads() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(small(&dir)).unwrap();

    for i in 0..5_000 {
        db.put(key(i), val(i), 0).unwrap();
    }
    db.flush_memtable().unwrap();
    db.compact_all().unwrap();

    let before = db.stats(0).unwrap();
    for i in 0..5_000 {
        // Keys that cannot exist, but which sit inside the files' key ranges.
        assert!(
            db.get(format!("key:{i:08}Z").as_bytes(), 0)
                .unwrap()
                .is_none()
        );
    }
    let after = db.stats(0).unwrap();

    let rejected = after.bloom_rejections - before.bloom_rejections;
    let read = after.sstable_reads - before.sstable_reads;
    assert!(
        rejected > read,
        "filters rejected {rejected} lookups but {read} still read a block; the filter is not earning its space"
    );
}
