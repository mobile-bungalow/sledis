use super::*;
use thiserror::*;

mod meta;
pub use self::meta::*;

pub trait TableReadStore: ReadStore {
    fn get_meta(&self, name: &[u8]) -> Result<Option<Meta>, Self::Error>;

    fn table_get(&self, name: &[u8], key: &[u8]) -> Result<Option<IVec>, Self::Error>;
}

pub trait TableWriteStore: WriteStore {
    fn table_insert<V>(&self, name: &[u8], key: &[u8], val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>;

    fn table_remove(&self, name: &[u8], key: &[u8]) -> Result<Option<IVec>, Self::Error>;

    fn table_fetch_update<V, F>(
        &self,
        name: &[u8],
        key: &[u8],
        f: F,
    ) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
        F: FnMut(Option<&[u8]>) -> Option<V>;
}

impl<S> TableReadStore for S
where
    S: ReadStore,
    S::Error: From<Error>,
{
    fn get_meta(&self, name: &[u8]) -> Result<Option<Meta>, Self::Error> {
        let key = Key::Table { name, key: None }.encode();

        if let Some(bs) = self.get(&key)? {
            if let Some(got) = Meta::decode(Segment::new(bs)) {
                Ok(Some(got))
            } else {
                Err(InvalidMeta(name.to_vec()).into())
            }
        } else {
            Ok(None)
        }
    }

    fn table_get(&self, name: &[u8], key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        let key = Key::Table {
            name,
            key: Some(key),
        }
        .encode();

        Ok(self.get(&key)?)
    }
}

fn update_table_meta<S, F>(store: &S, name: &[u8], mut f: F) -> Result<Option<Meta>, S::Error>
where
    S: WriteStore,
    S::Error: From<Error>,
    F: FnMut(Option<Meta>) -> Option<Meta>,
{
    let key = Key::Table { name, key: None }.encode();
    let mut err: Option<S::Error> = None;
    let mut meta: Option<Meta> = None;

    store.fetch_update::<IVec, _>(&key, |iv| {
        let got = if let Some(bs) = iv {
            if let Some(got) = Meta::decode(Segment::new(bs.into())) {
                err = None;
                Some(got)
            } else {
                err = Some(InvalidMeta(name.to_vec()).into());
                return Some(bs.into());
            }
        } else {
            None
        };

        let new = f(got);
        let res = new.as_ref().map(Meta::encode).map(IVec::from);
        meta = new;
        res
    })?;

    if let Some(e) = err {
        Err(e)
    } else {
        Ok(meta)
    }
}

impl<S> TableWriteStore for S
where
    S: WriteStore,
    S::Error: From<Error>,
{
    fn table_insert<V>(&self, name: &[u8], key: &[u8], val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
    {
        let key = Key::Table {
            name,
            key: Some(key),
        }
        .encode();

        let old = self.insert(&key, val)?;

        update_table_meta(self, name, |meta| {
            let mut meta = meta.unwrap_or_default();

            if old.is_none() {
                meta.len += 1;
            }

            Some(meta)
        })?;

        Ok(old)
    }

    fn table_remove(&self, name: &[u8], key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        let key = Key::Table {
            name,
            key: Some(key),
        }
        .encode();

        let old = self.remove(&key)?;

        update_table_meta(self, name, |meta| {
            meta.map(|mut meta| {
                if old.is_some() {
                    meta.len -= 1
                }
                meta
            })
        })?;

        Ok(old)
    }

    fn table_fetch_update<V, F>(
        &self,
        name: &[u8],
        key: &[u8],
        mut f: F,
    ) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
    {
        enum LenDiff {
            Incr,
            Decr,
            None,
        }
        let mut ldiff = LenDiff::None;

        let key = Key::Table {
            name,
            key: Some(key),
        }
        .encode();

        let old = self.fetch_update(&key, |old| {
            let new = f(old);
            ldiff = match (&old, &new) {
                (None, Some(_)) => LenDiff::Incr,
                (Some(_), None) => LenDiff::Decr,
                _ => LenDiff::None,
            };
            new
        })?;

        match ldiff {
            LenDiff::Incr => {
                update_table_meta(self, name, |meta| {
                    meta.map(|mut meta| {
                        meta.len += 1;
                        meta
                    })
                })?;
            }
            LenDiff::Decr => {
                update_table_meta(self, name, |meta| {
                    meta.map(|mut meta| {
                        meta.len -= 1;
                        meta
                    })
                })?;
            }
            LenDiff::None => {}
        }

        Ok(old)
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid table metadata, key was: {0:#?}")]
    InvalidMeta(Vec<u8>),
}

use self::Error::*;
