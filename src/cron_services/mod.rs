use crate::Conn;

use futures::{
    channel::oneshot, executor::ThreadPool, future::select, future::BoxFuture, FutureExt,
};
use futures_timer::Delay;
use sled::IVec;

use core::future::Future;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Provides an async API for scheduling delayed operations and cron jobs.
/// In order for spawned commands to execute they must be run on an async
/// runtime.
pub struct AsyncJobScheduler<'a> {
    conn: std::sync::Arc<Conn>,
    expiration_queue: Arc<RwLock<BTreeMap<IVec, oneshot::Sender<()>>>>,
    pub cleaning_job: BoxFuture<'a, ()>,
}

impl<'a> AsyncJobScheduler<'a> {
    /// Creates a new job scheduler with jobs to be managed by a user selected
    /// asynchronous runtime.
    pub fn new(conn: Arc<Conn>) -> Self {
        let mut sub = conn.ttl.watch_prefix(vec![]);
        let expiration_queue = Arc::new(RwLock::new(BTreeMap::new()));
        let cleaning_copy = expiration_queue.clone();
        Self {
            conn,
            expiration_queue,
            cleaning_job: Box::pin(async move {
                while let Some(event) = (&mut sub).await {
                    match event {
                        sled::Event::Remove { key } => match cleaning_copy.write() {
                            Ok(map) => {
                                map.remove(&key);
                            }
                            Err(_) => unreachable!(),
                        },
                        _ => {}
                    }
                }
            }),
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
            select(delay, rx);
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
            None
        } else {
            let conn = self.conn.clone();
            let key_clone = key.clone();
            let action_pin = Box::pin(async move {
                conn.as_ref().blob_remove(&key_clone);
            });
            let (handle, fut) = self.cron_inner(action_pin, delay);
            self.expiration_queue.write().unwrap().insert(key, handle);
            Some(fut)
        }
    }

    /// cancels the expiration of `key`. Returns Err if the key did not exist, Ok otherwise.
    pub fn persist_blob(&mut self, key: IVec) {
        if let Some(handle) = self.expiration_queue.get_mut(&key) {
            handle.send(());
            self.conn.ttl.remove(&key);
        }
    }
}

/// Provides an encapsulated run time for scheduling delayed operations on the
/// database
pub struct JobScheduler<'a> {
    inner: AsyncJobScheduler<'a>,
    pool: ThreadPool,
}

impl<'a> JobScheduler<'a> {
    pub fn new() -> () {}
    pub fn expire_key_at() -> () {}
    pub fn expire_key_in() -> () {}
    pub fn persist_key() -> () {}
}
