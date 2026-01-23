use super::{Ino, MetaKV};
use crate::utils::FS_BLK_SIZE;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub enum Itype {
    File,
    Dir,
    Symlink,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Inode {
    pub id: Ino,
    pub parent: Ino,
    pub kind: Itype,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub length: u64,
    pub links: u32,
}

impl Inode {
    pub fn blocks(&self) -> u64 {
        // block count in 512 bytes unit
        (self.length + 511) / 512
    }

    pub fn key(ino: Ino) -> String {
        format!("i_{}", ino)
    }

    pub fn val(this: &Self) -> Vec<u8> {
        bincode::serialize(this).expect("can't serialize inode")
    }
}

impl MetaKV for Inode {
    fn key(&self) -> String {
        Self::key(self.id)
    }

    fn val(&self) -> Vec<u8> {
        Self::val(self)
    }
}
