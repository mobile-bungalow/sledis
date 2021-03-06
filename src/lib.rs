use bytes::*;
use sled::IVec;

mod escaping;
pub use escaping::*;

mod keys;
pub use keys::*;

pub mod lists;

mod error;
pub use error::*;

pub trait Store {
    type Error: std::error::Error;

    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error>;

    fn insert<V>(&self, key: &[u8], val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>;

    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error>;

    fn fetch_update<V, F>(&self, key: &[u8], mut f: F) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
    {
        let got = self.get(key)?;
        let res = f(got.as_ref().map(IVec::as_ref));
        match res {
            Some(new) => self.insert(key, new),
            None => self.remove(key),
        }
    }
}

impl Store for sled::Tree {
    type Error = Error<sled::Error>;

    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        self.get(key).map_err(Error::Store)
    }

    fn insert<V>(&self, key: &[u8], val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
    {
        self.insert(key, val).map_err(Error::Store)
    }

    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        self.remove(key).map_err(Error::Store)
    }

    fn fetch_update<V, F>(&self, key: &[u8], f: F) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
    {
        self.fetch_and_update(key, f).map_err(Error::Store)
    }
}

impl Store for sled::TransactionalTree {
    type Error = Error<sled::ConflictableTransactionError>;

    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        self.get(key)
            .map_err(sled::ConflictableTransactionError::from)
            .map_err(Error::Store)
    }

    fn insert<V>(&self, key: &[u8], val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
    {
        self.insert::<&[u8], _>(key, val)
            .map_err(sled::ConflictableTransactionError::from)
            .map_err(Error::Store)
    }

    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        self.remove(key)
            .map_err(sled::ConflictableTransactionError::from)
            .map_err(Error::Store)
    }
}
