use thiserror::*;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    List(#[from] crate::list::ListError),
    #[error(transparent)]
    Table(#[from] crate::table::TableError),
    #[error("store error: {0}")]
    Store(#[from] sled::Error),
    #[error(transparent)]
    Cron(#[from] crate::cron_service::CronError),
}
