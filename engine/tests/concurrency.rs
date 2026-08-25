//! Concurrency stress for the scan path.
//!
//! These reproduce two bugs that unit tests could not: a scan that read the
//! version and the memtables at different instants, so a flush landing between
//! the two made the flushed records belong to neither and resurrected the
//! values they were shadowing; and a scan that opened a file compaction had
//! already deleted.
//!
//! Both only appear when background work runs concurrently with a read, which
//! is why they are here rather than in the main suite.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use engine::db::{Db, Options};
use engine::wal::SyncPolicy;
use tempfile::TempDir;

fn opts(dir: &TempDir) -> Options {
    Options {
        dir: dir.path().to_path_buf(),
        // Small enough that 500 keys force several flushes and a compaction,
        // which is what puts background work under the reader.
        memtable_bytes: 16 * 1024,
        block_size: 1024,
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

fn fill(db: &Db, n: usize) {
    for i in 0..n {
        db.put(
            format!("key:{i:08}").into_bytes(),
            format!("value-{i}-{}", "x".repeat(40)).into_bytes(),
            0,
        )
        .unwrap();
    }
}

#[test]
fn a_wipe_stays_wiped_while_the_background_thread_works() {
    for round in 0..6 {
        let dir = TempDir::new().unwrap();
        let db = Db::open(opts(&dir)).unwrap();
        fill(&db, 500);
        db.flush_all().unwrap();
        assert_eq!(
            db.exact_len(0).unwrap(),
            0,
            "round {round}: not empty right after the wipe"
        );

        db.put(b"after".to_vec(), b"wipe".to_vec(), 0).unwrap();

        // Keep checking while flushes and compactions run underneath. A wipe
        // that only holds until the next flush is not a wipe.
        for probe in 0..40 {
            let n = db.exact_len(0).unwrap();
            assert_eq!(
                n, 1,
                "round {round} probe {probe}: {n} keys came back after the wipe"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

#[test]
fn scanning_while_writing_never_sees_a_key_disappear() {
    // Every key written before the scan starts must be visible to it. A reader
    // that snapshots the version and the memtables at different instants can
    // miss exactly the records a concurrent flush is moving between them.
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Db::open(opts(&dir)).unwrap());
    fill(&db, 1_000);

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut i = 1_000;
            while !stop.load(Ordering::Relaxed) {
                db.put(format!("later:{i:08}").into_bytes(), b"v".to_vec(), 0)
                    .unwrap();
                i += 1;
            }
        })
    };

    for attempt in 0..25 {
        let mut seen = 0usize;
        let mut start = Vec::new();
        loop {
            let (page, next) = db.scan(&start, 200, 0).unwrap();
            for (k, _) in &page {
                if k.starts_with(b"key:") {
                    seen += 1;
                }
            }
            match next {
                Some(k) => start = k,
                None => break,
            }
        }
        assert_eq!(
            seen, 1_000,
            "attempt {attempt}: scan saw {seen} of the 1000 pre-existing keys"
        );
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
}

#[test]
fn reading_while_compacting_never_hits_a_deleted_file() {
    // Compaction deletes a file only after installing the version that stops
    // naming it. A reader holding a stale snapshot would open a file that is
    // no longer there and report an I/O error for a perfectly valid read.
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Db::open(opts(&dir)).unwrap());

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                // Rewriting the same key space keeps compaction busy.
                db.put(
                    format!("key:{:08}", i % 1_500).into_bytes(),
                    format!("v{i}{}", "y".repeat(30)).into_bytes(),
                    0,
                )
                .unwrap();
                i += 1;
            }
        })
    };

    for attempt in 0..150 {
        db.get(format!("key:{:08}", attempt % 1_500).as_bytes(), 0)
            .unwrap_or_else(|e| panic!("attempt {attempt}: point read failed: {e}"));
        let mut start = Vec::new();
        for _ in 0..5 {
            let (_, next) = db
                .scan(&start, 100, 0)
                .unwrap_or_else(|e| panic!("attempt {attempt}: scan failed: {e}"));
            match next {
                Some(k) => start = k,
                None => break,
            }
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
}
