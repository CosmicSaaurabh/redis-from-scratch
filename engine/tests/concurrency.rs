//! Concurrency tests for the read path.
//!
//! These pin two bugs that the rest of the suite could not reach, because both
//! need background work running *during* a read:
//!
//! - the scan captured the version and the memtables at two different instants,
//!   so a flush landing between them left the flushed records in neither and
//!   the scan returned the older values they had been shadowing. After a
//!   FLUSHALL that meant deleted keys coming back to life;
//! - compaction unlinked files a reader was still about to open.
//!
//! Both were verified by putting the bug back and watching these fail. A
//! regression test that cannot fail is worse than no test at all.
//!
//! # Why they are shaped like this
//!
//! Every test here is bounded by wall time, not by an operation count, and
//! asserts coverage on something the engine reports rather than on how many
//! iterations it managed. An earlier version asserted a minimum read count and
//! failed in CI when the machine was slower than expected, which is a test
//! reporting on the runner rather than on the code.
//!
//! The writer threads are rate limited for the same reason. Spinning as fast as
//! possible pins a core, and three of these running in parallel on a two-core
//! runner starved the engine's background thread so completely that the job
//! looked hung. Producing steady background work is the point; producing the
//! maximum possible amount of it is not.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use engine::db::{Db, Options};
use engine::wal::SyncPolicy;
use tempfile::TempDir;

/// How long each test lets background work and reads overlap.
const OVERLAP: Duration = Duration::from_secs(3);

/// How long the wipe test watches each round.
const WIPE_WATCH: Duration = Duration::from_millis(900);

/// How long it probes without pausing, covering the flush that carries the
/// tombstones out of the memtable.
const WIPE_TIGHT: Duration = Duration::from_millis(350);

/// Writes issued between pauses. Enough to keep flushes and compactions coming,
/// few enough to leave the machine some CPU.
const WRITE_BURST: usize = 40;

/// How long a writer pauses between bursts.
const WRITER_PAUSE: Duration = Duration::from_micros(300);

fn opts(dir: &TempDir) -> Options {
    Options {
        dir: dir.path().to_path_buf(),
        // Small enough that a few hundred keys force flushes and compaction,
        // which is what puts background work underneath the reader.
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

fn value(i: usize) -> Vec<u8> {
    format!("value-{i}-{}", "x".repeat(40)).into_bytes()
}

fn fill(db: &Db, n: usize) {
    for i in 0..n {
        db.put(format!("key:{i:08}").into_bytes(), value(i), 0)
            .unwrap();
    }
}

/// Spawns a rate-limited writer. It stops when `stop` is set.
fn spawn_writer(
    db: Arc<Db>,
    stop: Arc<AtomicBool>,
    written: Arc<AtomicU64>,
    key_for: impl Fn(usize) -> Vec<u8> + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut i = 0usize;
        while !stop.load(Ordering::Relaxed) {
            for _ in 0..WRITE_BURST {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                db.put(key_for(i), value(i), 0).unwrap();
                i += 1;
            }
            written.store(i as u64, Ordering::Relaxed);
            std::thread::sleep(WRITER_PAUSE);
        }
    })
}

#[test]
fn a_wipe_stays_wiped_while_the_background_thread_works() {
    // FLUSHALL writes a tombstone for every key. Those tombstones then travel
    // from the memtable into level 0 and onward through compaction, and the
    // wipe has to hold for the whole journey. It did not: a scan that read the
    // version and the memtables at different instants missed the tombstones
    // while they were in flight and returned the values underneath them.
    for round in 0..8 {
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

        // The window this is hunting is narrow: it opens only while a flush is
        // actually moving the tombstones out of a memtable, which is in the
        // first moments after the wipe. So the probing is tight to begin with
        // and then backs off, rather than being evenly paced - an evenly paced
        // loop with a couple of milliseconds between probes missed the bug
        // entirely even while running thousands of times.
        let started = Instant::now();
        let mut probes = 0u64;
        while started.elapsed() < WIPE_WATCH {
            let n = db.exact_len(0).unwrap();
            assert_eq!(
                n, 1,
                "round {round} probe {probes}: {n} keys came back after the wipe"
            );
            probes += 1;
            if started.elapsed() > WIPE_TIGHT {
                std::thread::sleep(Duration::from_millis(2));
            }
        }

        // Coverage: the tombstones must actually have reached disk during the
        // window, or the test watched nothing happen.
        let stats = db.stats(0).unwrap();
        assert!(
            stats.flushes > 0,
            "round {round}: nothing was flushed, so the wipe never left the memtable"
        );
    }
}

#[test]
fn scanning_while_writing_never_sees_a_key_disappear() {
    // Every key written before a scan starts must be visible to it. A reader
    // that captures the version and the memtables at different instants misses
    // exactly the records a concurrent flush is moving between them.
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Db::open(opts(&dir)).unwrap());
    fill(&db, 1_000);

    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let writer = spawn_writer(
        Arc::clone(&db),
        Arc::clone(&stop),
        Arc::clone(&written),
        |i| format!("later:{i:08}").into_bytes(),
    );

    let deadline = Instant::now() + OVERLAP;
    let mut scans = 0u64;
    while Instant::now() < deadline {
        let mut seen = 0usize;
        let mut start = Vec::new();
        loop {
            let (page, next) = db.scan(&start, 200, 0).unwrap();
            seen += page.iter().filter(|(k, _)| k.starts_with(b"key:")).count();
            match next {
                Some(k) => start = k,
                None => break,
            }
        }
        assert_eq!(
            seen, 1_000,
            "scan {scans} saw {seen} of the 1000 pre-existing keys"
        );
        scans += 1;
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert!(
        written.load(Ordering::Relaxed) > 0,
        "the writer never ran alongside the scans"
    );
}

#[test]
fn reading_while_compacting_never_hits_a_deleted_file() {
    // Compaction unlinks the files a version stops naming. A reader holding a
    // snapshot of that version would then open a file that is no longer there
    // and fail a perfectly valid read.
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Db::open(opts(&dir)).unwrap());
    fill(&db, 3_000);

    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    // Rewriting one key space over and over is what keeps compaction
    // continuously producing and retiring files.
    let writer = spawn_writer(
        Arc::clone(&db),
        Arc::clone(&stop),
        Arc::clone(&written),
        |i| format!("key:{:08}", i % 3_000).into_bytes(),
    );

    let deadline = Instant::now() + OVERLAP;
    let mut read = 0usize;
    while Instant::now() < deadline {
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
        read += 1;
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    // Coverage is asserted on compaction having run, not on a read count: a
    // read count measures the machine, and asserting on it is what made an
    // earlier version of this test fail in CI for no real reason.
    let stats = db.stats(0).unwrap();
    assert!(
        stats.compactions > 0,
        "no compaction ran, so no file was ever retired underneath a reader"
    );
}
