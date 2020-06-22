use super::*;
use thiserror::*;

mod meta;
pub use self::meta::*;

impl Conn {
    pub fn table_get_meta(&self, name: &[u8]) -> Result<Meta, Error> {
        let key = keys::table_meta(name);

        if let Some(bs) = self.items.get(&key)? {
            Meta::decode(Segment::new(bs)).ok_or_else(|| InvalidMeta(name.to_vec()).into())
        } else {
            Ok(Meta::default())
        }
    }

    pub fn table_get(&self, name: &[u8], key: &[u8]) -> Result<Option<IVec>, Error> {
        Ok(self.items.get(&keys::table(name, key))?)
    }

    #[inline]
    pub fn table_update<F: for<'a> FnOnce(&'a Meta, &'a Option<IVec>) -> Option<IVec>>(
        &self,
        name: &[u8],
        key: &[u8],
        f: F,
    ) -> Result<Option<IVec>, Error> {
        let meta_key = IVec::from(keys::table_meta(name));
        let key = IVec::from(keys::table(name, key));

        let mutex = self.locks.lock(&meta_key);
        let _guard = mutex.write();

        let old = self.items.get(&key)?;
        let mut meta = self.table_get_meta(name)?;

        let new = f(&meta, &old);

        match (&old, &new) {
            (None, Some(_)) => {
                meta.len += 1;
            }
            (Some(_), None) => {
                meta.len -= 1;
            }
            _ => {}
        }

        let mut batch = sled::Batch::default();
        batch.insert(&meta_key, meta.encode());
        if let Some(iv) = new {
            batch.insert(&key, iv);
        } else {
            batch.remove(&key);
        }
        self.items.apply_batch(batch)?;

        Ok(old)
    }

    pub fn table_insert(&self, name: &[u8], key: &[u8], val: IVec) -> Result<Option<IVec>, Error> {
        self.table_update(name, key, move |_, _| Some(val))
    }

    pub fn table_remove(&self, name: &[u8], key: &[u8]) -> Result<Option<IVec>, Error> {
        self.table_update(name, key, move |_, _| None)
    }
}

#[derive(Error, Debug)]
pub enum TableError {
    #[error("invalid table metadata, key was: {0:#?}")]
    InvalidMeta(Vec<u8>),
}

use self::TableError::*;
