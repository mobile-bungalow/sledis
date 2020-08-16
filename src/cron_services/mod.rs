use crate::Conn;

use futures::{
    channel::oneshot, executor::ThreadPool, future::select, future::BoxFuture, FutureExt,
};
use futures_timer::Delay;
use sled::IVec;

use std::collections::BTreeMap;
use std::future::Future;
use std::time::{Duration, Instant};

/// Provides an async API for scheduling delayed operations and cron jobs.
/// In order for spawned commands to execute they must be run on an async
/// runtime.
pub struct AsyncJobScheduler<C: AsRef<Conn> + Clone> {
    conn: C,
    expiration_queue: BTreeMap<IVec, oneshot::Sender<()>>,
}

impl<C> AsyncJobScheduler<C>
where
    C: AsRef<Conn> + Clone,
{
    /// Creates a new job scheduler with jobs to be managed by a user selected
    /// asynchronous runtime.
    pub fn new(conn: C) -> Self {
        Self {
            conn,
            expiration_queue: BTreeMap::new(),
        }
    }

    /// a generic inner function for running cron jobs.
    fn cron_inner<'a>(
        &self,
        action: BoxFuture<'a, ()>,
        delay: Duration,
    ) -> (oneshot::Sender<()>, impl Future<Output = ()> + 'a) {
        let (tx, rx) = oneshot::channel();

        let fut = async move {
            let delay = Delay::new(delay).then(|_| action);
            select(delay, rx);
        };

        (tx, fut)
    }

    /// Schedules the key to be deleted at `instant`. If instant is in the past
    /// the key is deleted immediately. If this is called on a key with a preexisting
    /// expiration date, the date is updated.
    pub fn expire_key_at(&self, key: IVec, instant: Instant) -> impl Future<Output = ()> {
        async {}
    }
    /// Schedules `key` to be deleted after `duration`. If this is called on a key with a preexisting
    /// expiration date, the date is updated.
    pub fn expire_key_in(&self, key: IVec, delay: Duration) -> impl Future<Output = ()> {
        let conn = self.clone();
        let (handle, fut) = self.cron_inner(Box::pin(async move {}), delay);
        fut
    }

    /// cancels the expiration of `key`. Returns Err if the key did not exist, Ok otherwise.
    pub fn persist_key(&self) -> Result<(), ()> {
        Ok(())
    }
}

/// Provides an encapsulated run time for scheduling delayed operations on the
/// database
pub struct JobScheduler<C: AsRef<Conn> + Clone> {
    inner: AsyncJobScheduler<C>,
    pool: ThreadPool,
}

impl<C> JobScheduler<C>
where
    C: AsRef<Conn> + Clone,
{
    pub fn new() -> () {}
    pub fn expire_key_at() -> () {}
    pub fn expire_key_in() -> () {}
    pub fn persist_key() -> () {}
}
