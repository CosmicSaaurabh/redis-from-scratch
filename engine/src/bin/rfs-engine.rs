//! The storage engine server.
//!
//! Runs the LSM tree and serves it over gRPC to the Go node. It is a separate
//! process on purpose: a fault in the storage engine takes down the storage
//! engine, not the server that was talking to it, and both can be profiled and
//! restarted independently.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use engine::db::{Db, Options};
use engine::service::EngineService;
use engine::service::pb::storage_engine_server::StorageEngineServer;
use engine::wal::SyncPolicy;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

/// Command line configuration.
#[derive(Parser, Debug)]
#[command(
    name = "rfs-engine",
    version,
    about = "LSM storage engine for redis-from-scratch"
)]
struct Args {
    /// Address to serve gRPC on.
    #[arg(long, env = "RFS_ENGINE_ADDR", default_value = "127.0.0.1:50051")]
    addr: SocketAddr,

    /// Data directory.
    #[arg(long, env = "RFS_ENGINE_DIR", default_value = "./engine-data")]
    dir: PathBuf,

    /// When log bytes are forced to stable storage: always, everysec or no.
    #[arg(long, env = "RFS_ENGINE_FSYNC", default_value = "everysec")]
    fsync: String,

    /// Freeze the memtable once it passes this many bytes.
    #[arg(long, env = "RFS_ENGINE_MEMTABLE_BYTES", default_value_t = 64 << 20)]
    memtable_bytes: usize,

    /// Start a level-0 compaction once this many files accumulate.
    #[arg(long, default_value_t = 4)]
    l0_trigger: usize,

    /// Stall writers once level 0 reaches this many files.
    #[arg(long, default_value_t = 12)]
    l0_stop: usize,

    /// Byte budget for level 1.
    #[arg(long, default_value_t = 64 << 20)]
    level_base_bytes: u64,

    /// Bloom filter bits per key.
    #[arg(long, default_value_t = 10)]
    bits_per_key: usize,

    /// Log level.
    #[arg(long, env = "RFS_ENGINE_LOG", default_value = "info")]
    log: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log)),
        )
        .init();

    let opts = Options {
        dir: args.dir.clone(),
        memtable_bytes: args.memtable_bytes,
        bits_per_key: args.bits_per_key,
        sync_policy: SyncPolicy::parse(&args.fsync)?,
        l0_compaction_trigger: args.l0_trigger,
        l0_stop_writes_trigger: args.l0_stop,
        level_base_bytes: args.level_base_bytes,
        ..Options::default()
    };

    tracing::info!(
        addr = %args.addr, dir = %args.dir.display(), fsync = %args.fsync,
        memtable_bytes = args.memtable_bytes, "starting storage engine"
    );

    let db = Arc::new(Db::open(opts)?);
    let service = EngineService::new(Arc::clone(&db), env!("CARGO_PKG_VERSION"));

    // The engine holds the only durable copy of the data, so shutdown is not
    // optional housekeeping: it flushes and fsyncs the log. Wiring it to the
    // signal rather than letting the process be killed outright is the
    // difference between a clean start and a log replay on every restart.
    // Both signals, not just ctrl-c. Containers and process managers send
    // SIGTERM, and an engine that ignores it is killed without flushing, which
    // turns every orchestrated restart into a log replay.
    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "cannot install the SIGTERM handler");
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received"),
                _ = term.recv() => tracing::info!("SIGTERM received"),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received");
        }
    };

    let result = Server::builder()
        .add_service(StorageEngineServer::new(service))
        .serve_with_shutdown(args.addr, shutdown)
        .await;

    tracing::info!("closing the storage engine");
    if let Err(e) = db.close() {
        tracing::error!(error = %e, "closing the engine failed");
    }
    result?;
    Ok(())
}
