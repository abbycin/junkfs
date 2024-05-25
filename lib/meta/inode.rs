use crate::utils::FS_BLK_SIZE;
use super::Ino;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Slice {
    pos: u32,    // offset from chunk start
    length: u32, // continuous data length
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Chunk {
    index: u32,         // which chunk in file
    length: u32,        // total length of used bytes in chunk
    slices: Vec<Slice>, // scatter-gather small un-sequence blocks data in chunk
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum Itype {
    File,
    Link,
    Dir,
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
    pub chunks: Vec<Chunk>,
}

impl Inode {
    pub fn blocks(&self) -> u64 {
        self.length / FS_BLK_SIZE + (if self.length % FS_BLK_SIZE > 0 { 1 } else { 0 })
    }

    pub fn se(&self) -> Vec<u8> {
        bincode::serialize(self).expect("can't serialize inode")
    }
}
