use super::*;

pub type ListIndex = i128;
pub const INDEX_BYTES: usize = 16;

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub enum Key<'a> {
    Blob(&'a [u8]),
    List(&'a [u8], ListIndex),
    ListMeta(&'a [u8]),
    SubMap(&'a [u8], &'a [u8]),
    MapMeta(&'a [u8]),
}

const BLOB_TAG: u8 = 2;
const LIST_TAG: u8 = 3;
const LIST_META_TAG: u8 = 4;
const MAP_TAG: u8 = 5;
const MAP_META_TAG: u8 = 6;

pub fn encode_list_index(i: ListIndex) -> [u8; INDEX_BYTES] {
    (i ^ ListIndex::min_value()).to_be_bytes()
}

pub fn decode_list_index(inp: &[u8]) -> Option<ListIndex> {
    if inp.len() != INDEX_BYTES {
        return None;
    }

    let mut buf = [0u8; INDEX_BYTES];
    buf.copy_from_slice(inp);

    Some(ListIndex::min_value() ^ ListIndex::from_be_bytes(buf))
}

impl<'a> Key<'a> {
    pub fn encode(&self) -> Vec<u8> {
        let mut out;
        match self {
            Key::Blob(b) => {
                out = Vec::with_capacity(1 + b.len() + 2);
                out.push(BLOB_TAG);
                out.extend_from_slice(escape(b).as_ref());
            }
            Key::List(k, ix) => {
                out = Vec::with_capacity(1 + k.len() + 2 + INDEX_BYTES);
                out.push(LIST_TAG);
                out.extend_from_slice(escape(k).as_ref());
                let ix_bytes = encode_list_index(*ix);
                out.extend_from_slice(&ix_bytes);
            }
            Key::ListMeta(k) => {
                out = Vec::with_capacity(1 + k.len() + 2);
                out.push(LIST_META_TAG);
                out.extend_from_slice(escape(k).as_ref());
            }
            Key::SubMap(k1, k2) => {
                out = Vec::with_capacity(1 + k1.len() + 2 + k2.len() + 2);
                out.push(MAP_TAG);
                out.extend_from_slice(escape(k1).as_ref());
                out.extend_from_slice(escape(k2).as_ref());
            }
            Key::MapMeta(k) => {
                out = Vec::with_capacity(1 + k.len() + 2);
                out.push(MAP_META_TAG);
                out.extend_from_slice(escape(k).as_ref());
            }
        }
        out
    }
}
