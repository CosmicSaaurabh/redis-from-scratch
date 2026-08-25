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

/// A safety cap so a writer thread terminates even if its reader dies.
///
/// The reader decides when each test ends, not the writer. Making the writer
/// stop after a fixed count instead was a mistake: on a fast machine it
/// finished before the reader had done any meaningful work, and the test's own
/// coverage assertion fired. Writers also yield between operations, because an
/// unbounded tight loop pins a core and three of these in parallel on a
/// two-core runner starve the engine's background thread.
const WRITER_SAFETY_CAP: usize = 400_000;

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
    // Compaction unlinks the files a version stops naming. A reader holding a
    // snapshot of that version would then open a file that is no longer there
    // and report an I/O error for a perfectly valid read.
    //
    // The reader has to be running *while* compactions install and retire
    // files, so the tree is pre-filled to guarantee there is something to
    // compact, and the reader - not the writer - decides when the test ends.
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Db::open(opts(&dir)).unwrap());
    fill(&db, 3_000);

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        std::thread::spawn(move || {
            // Rewriting one key space over and over is what keeps compaction
            // continuously producing and retiring files.
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) && i < WRITER_SAFETY_CAP {
                db.put(
                    format!("key:{:08}", i % 3_000).into_bytes(),
                    format!("v{i}{}", "y".repeat(30)).into_bytes(),
                    0,
                )
                .unwrap();
                i += 1;
                std::thread::yield_now();
            }
        })
    };

    for read in 0..120 {
        let k = format!("key:{:08}", read % 3_000);
        db.get(k.as_bytes(), 0)
            .unwrap_or_else(|e| panic!("point read {read} failed while compaction ran: {e}"));

        let mut start = Vec::new();
        for page in 0..4 {
            let (_, next) = db.scan(&start, 128, 0).unwrap_or_else(|e| {
                panic!("scan page {page} of read {read} failed while compaction ran: {e}")
            });
            match next {
                Some(k) => start = k,
                None => break,
            }
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    // The coverage check is that compaction actually ran, not that some number
    // of reads happened: a read count depends on how fast the machine is, and
    // asserting on it is what made this test fail in CI for no real reason.
    let stats = db.stats(0).unwrap();
    assert!(
        stats.compactions > 0,
        "no compaction ran, so no file was ever retired under a reader"
    );
}
