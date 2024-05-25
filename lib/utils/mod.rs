mod bitmap;

use crate::meta::{Ino, Inode, Itype};
pub use bitmap::BitMap;
use fuser::{FileAttr, FileType};
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};
use once_cell::sync::Lazy;

pub const CHUNK_SIZE: u64 = 1 << 26;
pub const BLOCK_SIZE: u64 = 1 << 22;
pub const FS_BLK_SIZE: u64 = 4096;
pub const FS_TOTAL_INODES: u32 = 1 << 20;

static mut DATA_PATH: Lazy<String> = Lazy::new(|| {"".to_string()});

pub fn init_data_path(mp: String) {
    unsafe {
        *DATA_PATH = mp;
    }
}

pub fn get_data_path() -> &'static String {
    unsafe {
        &*DATA_PATH
    }
}

pub fn to_systime(s: u64) -> SystemTime {
    UNIX_EPOCH + time::Duration::from_secs(s)
}

pub fn to_filetype(s: Itype) -> FileType {
    match s {
        Itype::File => FileType::RegularFile,
        Itype::Link => FileType::Symlink,
        Itype::Dir => FileType::Directory,
    }
}

pub fn to_attr(inode: &Inode) -> FileAttr {
    FileAttr {
        ino: inode.id,
        size: inode.length,
        blocks: inode.blocks(),
        atime: to_systime(inode.atime),
        mtime: to_systime(inode.mtime),
        ctime: to_systime(inode.ctime),
        kind: to_filetype(inode.kind),
        perm: inode.mode,
        nlink: inode.links,
        uid: inode.uid,
        gid: inode.gid,
        blksize: FS_BLK_SIZE as u32,
        // the following is unused
        rdev: 0,
        crtime: time::SystemTime::now(),
        flags: 0,
    }
}

pub fn sb_key() -> String {
    "superblock".to_string()
}

pub fn inode_key(inode: Ino) -> String {
    format!("inode_{inode}")
}
