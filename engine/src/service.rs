//! The gRPC service exposing the storage engine.
//!
//! Every handler is a thin translation between protobuf messages and the
//! engine's own types. Deliberately thin: any logic that lives here would be
//! logic the engine's own tests never cover, and the tests that matter drive
//! [`crate::db::Db`] directly.
//!
//! Engine calls block on real I/O, so they run on Tokio's blocking pool rather
//! than on an async worker. Doing a synchronous fsync on an async worker thread
//! stalls every other task sharing it, and under load that turns one slow disk
//! write into a server-wide latency spike.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::db::Db;
use crate::error::Error;
use crate::types::{Entry, Mutation, Record};

/// The generated protobuf types and service stubs.
pub mod pb {
    #![allow(missing_docs)]
    tonic::include_proto!("rfs.engine.v1");
}

use pb::storage_engine_server::StorageEngine;

/// Serves a [`Db`] over gRPC.
pub struct EngineService {
    db: Arc<Db>,
    version: String,
}

impl EngineService {
    /// Wraps a database.
    pub fn new(db: Arc<Db>, version: impl Into<String>) -> Self {
        EngineService {
            db,
            version: version.into(),
        }
    }

    /// Runs `f` on the blocking pool with the database in scope.
    async fn blocking<T, F>(&self, f: F) -> Result<T, Status>
    where
        F: FnOnce(&Db) -> crate::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || f(&db))
            .await
            .map_err(|e| Status::internal(format!("engine task panicked: {e}")))?
            .map_err(to_status)
    }
}

/// Maps an engine error onto a gRPC status.
///
/// The mapping matters: the Go client retries some codes and not others, and
/// retrying a corruption error would just read the same bad bytes again.
fn to_status(e: Error) -> Status {
    match e {
        Error::InvalidArgument(m) => Status::invalid_argument(m),
        Error::Closed => Status::unavailable("engine is shutting down"),
        Error::Poisoned(m) => {
            Status::failed_precondition(format!("engine has failed and is refusing writes: {m}"))
        }
        // Corruption is reported as data loss rather than internal: it is
        // permanent, and a client that retries it will get the same answer.
        e if e.is_corruption() => Status::data_loss(e.to_string()),
        e => Status::internal(e.to_string()),
    }
}

fn to_record(r: Record) -> pb::Record {
    pb::Record {
        value: r.value.to_vec(),
        expire_at: r.expire_at,
    }
}

fn from_record(r: Option<pb::Record>) -> Record {
    match r {
        Some(r) => Record {
            value: r.value.into(),
            expire_at: r.expire_at,
        },
        None => Record {
            value: Vec::new().into(),
            expire_at: 0,
        },
    }
}

#[tonic::async_trait]
impl StorageEngine for EngineService {
    async fn get(&self, req: Request<pb::GetRequest>) -> Result<Response<pb::GetResponse>, Status> {
        let req = req.into_inner();
        let found = self
            .blocking(move |db| db.get(&req.key, req.now_ms))
            .await?;
        Ok(Response::new(pb::GetResponse {
            found: found.is_some(),
            record: found.map(to_record),
        }))
    }

    async fn put(&self, req: Request<pb::PutRequest>) -> Result<Response<pb::PutResponse>, Status> {
        let req = req.into_inner();
        let rec = from_record(req.record);
        let seq = self
            .blocking(move |db| db.put(req.key, rec.value.to_vec(), rec.expire_at))
            .await?;
        Ok(Response::new(pb::PutResponse { sequence: seq }))
    }

    async fn delete(
        &self,
        req: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        let req = req.into_inner();
        let (existed, seq) = self
            .blocking(move |db| {
                // Whether the key existed has to be read before the tombstone
                // goes down, because afterwards the engine cannot tell a key
                // that was deleted from one that was never there.
                let existed = db.get(&req.key, req.now_ms)?.is_some();
                let seq = db.delete(req.key)?;
                Ok((existed, seq))
            })
            .await?;
        Ok(Response::new(pb::DeleteResponse {
            existed,
            sequence: seq,
        }))
    }

    async fn batch_write(
        &self,
        req: Request<pb::BatchWriteRequest>,
    ) -> Result<Response<pb::BatchWriteResponse>, Status> {
        let req = req.into_inner();
        let muts: Vec<Mutation> = req
            .mutations
            .into_iter()
            .map(|m| {
                if m.delete {
                    Mutation::delete(m.key)
                } else {
                    let r = from_record(m.record);
                    Mutation {
                        key: m.key,
                        entry: Entry::Put(r),
                    }
                }
            })
            .collect();
        let seq = self.blocking(move |db| db.write(muts)).await?;
        Ok(Response::new(pb::BatchWriteResponse { sequence: seq }))
    }

    async fn scan(
        &self,
        req: Request<pb::ScanRequest>,
    ) -> Result<Response<pb::ScanResponse>, Status> {
        let req = req.into_inner();
        let limit = (req.limit as usize).clamp(1, 100_000);
        let (page, next) = self
            .blocking(move |db| db.scan(&req.start, limit, req.now_ms))
            .await?;
        Ok(Response::new(pb::ScanResponse {
            entries: page
                .into_iter()
                .map(|(k, r)| pb::KeyValue {
                    key: k,
                    record: Some(to_record(r)),
                })
                .collect(),
            has_more: next.is_some(),
            next_start: next.unwrap_or_default(),
        }))
    }

    async fn flush_all(
        &self,
        _req: Request<pb::FlushAllRequest>,
    ) -> Result<Response<pb::FlushAllResponse>, Status> {
        let removed = self
            .blocking(move |db| {
                let before = db.estimated_len();
                db.flush_all()?;
                Ok(before)
            })
            .await?;
        Ok(Response::new(pb::FlushAllResponse {
            keys_removed: removed,
        }))
    }

    async fn sync(
        &self,
        _req: Request<pb::SyncRequest>,
    ) -> Result<Response<pb::SyncResponse>, Status> {
        self.blocking(move |db| db.sync()).await?;
        Ok(Response::new(pb::SyncResponse {}))
    }

    async fn flush(
        &self,
        _req: Request<pb::FlushRequest>,
    ) -> Result<Response<pb::FlushResponse>, Status> {
        self.blocking(move |db| db.flush_memtable()).await?;
        Ok(Response::new(pb::FlushResponse {}))
    }

    async fn compact(
        &self,
        _req: Request<pb::CompactRequest>,
    ) -> Result<Response<pb::CompactResponse>, Status> {
        self.blocking(move |db| db.compact_all()).await?;
        Ok(Response::new(pb::CompactResponse {}))
    }

    async fn stats(
        &self,
        req: Request<pb::StatsRequest>,
    ) -> Result<Response<pb::StatsResponse>, Status> {
        let now_ms = req.into_inner().now_ms;
        let version = self.version.clone();
        let (s, truncated, batches) = self
            .blocking(move |db| {
                let s = db.stats(now_ms)?;
                let r = db.recovery().clone();
                Ok((s, r.truncated, r.log_batches))
            })
            .await?;
        Ok(Response::new(pb::StatsResponse {
            keys_estimate: s.keys,
            disk_bytes: s.disk_bytes,
            memory_bytes: s.memory_bytes,
            level_files: s.level_files.iter().map(|n| *n as u64).collect(),
            level_bytes: s.level_bytes.clone(),
            writes: s.writes,
            reads: s.reads,
            flushes: s.flushes,
            compactions: s.compactions,
            compaction_bytes_read: s.compaction_bytes_read,
            compaction_bytes_written: s.compaction_bytes_written,
            write_stalls: s.write_stalls,
            bloom_rejections: s.bloom_rejections,
            sstable_reads: s.sstable_reads,
            wal_appends: s.wal.appends,
            wal_writes: s.wal.writes,
            wal_fsyncs: s.wal.fsyncs,
            wal_bytes: s.wal.bytes_out,
            wal_unsynced: s.wal.last_sequence.saturating_sub(s.wal.synced_sequence),
            wal_policy: s.wal.policy.as_str().to_string(),
            version,
            recovery_truncated: truncated,
            recovery_log_batches: batches,
        }))
    }
}
