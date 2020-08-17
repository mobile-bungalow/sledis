use crate::Conn;
use crate::Error;
use thiserror::Error;

use futures::{
    channel::oneshot,
    executor::{block_on, ThreadPool},
    future::select,
    future::BoxFuture,
    lock::Mutex,
    FutureExt,
};
use futures_timer::Delay;
use sled::IVec;

use core::future::Future;
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

#[derive(Error, Debug)]
pub enum CronError {
    #[error("Internal sled error trying to acquire ttl {0:?}")]
    TtlAcq(sled::Error),
    #[error("could not convert bytes stored in ttl tree to system time {0:?}")]
    SystemTimeFormat(Vec<u8>),
    #[error("Could not initialize thread pool for cron service {0:?}")]
    ThreadPoolInit(#[from] std::io::Error),
}

struct SysTime(SystemTime);

impl TryFrom<&[u8]> for SysTime {
    type Error = Vec<u8>;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        // deserialize into u128 from ttl tree
        let duration_since_epoch_millis =
            u128::from_le_bytes(<[u8; 16]>::try_from(value).map_err(|_| value.to_vec())?);

        // downcast to u64 for duration from millis
        let downcast: u64 = duration_since_epoch_millis
            .try_into()
            .map_err(|_| value.to_vec())?;

        // cast to SystemTime
        let dur = Duration::from_millis(downcast);
        Ok(SysTime(std::time::UNIX_EPOCH + dur))
    }
}

/// Provides an async API for scheduling delayed operations and cron jobs.
/// In order for spawned commands to execute they must be run on an async
/// runtime.
pub struct AsyncJobScheduler {
    conn: Arc<Conn>,
    expiration_queue: Arc<Mutex<BTreeMap<IVec, oneshot::Sender<()>>>>,
}

impl AsyncJobScheduler {
    /// Creates a new job scheduler with jobs to be managed by a user selected
    /// asynchronous runtime.
    pub fn new(conn: Arc<Conn>) -> Self {
        Self {
            conn,
            expiration_queue: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// a generic inner function for running cron jobs.
    fn cron_inner<'b>(
        &self,
        action: BoxFuture<'b, ()>,
        delay: Duration,
    ) -> (oneshot::Sender<()>, impl Future<Output = ()> + 'b) {
        let (tx, rx) = oneshot::channel();

        let fut = async move {
            let delay = Delay::new(delay).then(|_| action);
            select(delay, rx).await;
        };

        (tx, fut)
    }

    /// Schedules the blob entry at `key` to be deleted at `instant`. If instant is in the past
    /// the key is deleted immediately. If this is called on a key with a preexisting
    /// expiration date, the date is updated.
    pub fn expire_blob_at(
        &mut self,
        key: IVec,
        instant: Instant,
    ) -> Option<impl Future<Output = ()>> {
        let dur = instant.duration_since(Instant::now());
        self.expire_blob_in(key, dur)
    }

    /// Schedules blob entry at `key` to be deleted after `duration`. If this is called on a key with a preexisting
    /// expiration date, the date is updated.
    pub fn expire_blob_in(
        &mut self,
        key: IVec,
        delay: Duration,
    ) -> Option<impl Future<Output = ()>> {
        if delay == Duration::from_secs(0) {
            let _ = self.conn.blob_remove(&key);
            None
        } else {
            let conn = self.conn.clone();
            let exp_queue_clone = self.expiration_queue.clone();
            let key_clone = key.clone();

            let action_pin = Box::pin(async move {
                let _ = conn.as_ref().blob_remove(&key_clone);
                let _ = conn.ttl.remove(&key_clone);
                exp_queue_clone.lock().await.remove(&key_clone);
            });

            let (handle, fut) = self.cron_inner(action_pin, delay);

            let time_ms = (SystemTime::now() + delay)
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap() // TODO: PANICS HERE. THE TYPES: THEY ARE WRONG AAAHHHH
                .as_millis();

            // set the new expiration date
            let _ = self.conn.ttl.insert(key.as_ref(), &time_ms.to_le_bytes());
            // if there was a preexisting future for this key, kill it.
            if let Some(old_handle) = block_on(self.expiration_queue.lock()).insert(key, handle) {
                let _ = old_handle.send(());
            }

            Some(fut)
        }
    }

    /// cancels the expiration of `key`. Returns Err if there was an issue removing the key from
    /// ttl tree. If the key was present and had a valid timestamp Some(time) will be returned,
    /// otherwise None.
    pub fn persist_blob(&mut self, key: IVec) -> Result<Option<SystemTime>, Error> {
        if let Some(handle) = block_on(self.expiration_queue.lock()).remove(&key) {
            // failure means it has already been removed
            let _ = handle.send(());
            let expiration_time = self.conn.ttl.remove(&key)?;
            if let Some(byte_rep) = expiration_time {
                Ok(SysTime::try_from(byte_rep.as_ref())
                    .map(|wrapper| wrapper.0)
                    .ok())
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

// TODO: improve docs with links
/// Provides an encapsulated run time for scheduling delayed operations on the
/// database.
pub struct JobScheduler {
    inner: AsyncJobScheduler,
    pool: ThreadPool,
}

impl JobScheduler {
    /// Create a new job scheduler and runtime.
    pub fn new(conn: Arc<Conn>) -> Result<Self, CronError> {
        Ok(Self {
            inner: AsyncJobScheduler::new(conn),
            pool: ThreadPool::new()?,
        })
    }

    /// attempts to schedule a blob for deletion at an instant in the future.
    /// if the instant is in the past. the key is deleted immediately.
    /// if the key is nonexistant. nothing is scheduled.
    pub fn expire_blob_at(&mut self, key: IVec, instant: Instant) -> Result<(), Error> {
        if let Some(fut) = self.inner.expire_blob_at(key, instant) {
            self.pool.spawn_ok(fut);
        }
        Ok(())
    }

    pub fn expire_blob_in(&mut self, key: IVec, delay: Duration) -> Result<(), Error> {
        if let Some(fut) = self.inner.expire_blob_in(key, delay) {
            self.pool.spawn_ok(fut);
        }
        Ok(())
    }

    pub fn persist_blob(&mut self, key: IVec) -> Result<Option<SystemTime>, Error> {
        self.inner.persist_blob(key)
    }
}
