use crate::meta::{Ino, MetaItem};
use crate::utils::{BitMap, FS_ROOT_INODE, FS_TOTAL_INODES};
use serde::{Deserialize, Serialize};

// NOTE: we use a key-value database to store metadata of filesystem, so it's unnecessary to store
// inode map, data map and inode table in metadata, we only limit the total number of data blocks
// and inode count is enough
#[derive(Serialize, Deserialize, Debug)]
pub struct SuperBlock {
    ino: Ino,
    uri: String, // currently the `uri` is a path to store file blocks
    imap: BitMap,
}

impl SuperBlock {
    pub fn new(uri: &str) -> Self {
        SuperBlock {
            ino: FS_ROOT_INODE,
            uri: uri.to_string(),
            imap: BitMap::new(FS_TOTAL_INODES as u64),
        }
    }

    pub fn alloc_ino(&mut self) -> Option<Ino> {
        self.imap.alloc()
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn free_ino(&mut self, ino: Ino) {
        self.imap.del(ino);
    }

    pub fn key() -> String {
        "sb".to_string()
    }

    pub fn val(this: &Self) -> Vec<u8> {
        bincode::serialize(this).expect("can't serialize superblock")
    }
}

impl MetaItem for SuperBlock {
    fn key(&self) -> String {
        Self::key()
    }

    fn val(&self) -> Vec<u8> {
        Self::val(self)
    }
}
