mod bitmap;

use crate::meta::{Inode, Itype};
pub use bitmap::BitMap;
use fuser::{FileAttr, FileType};
use once_cell::sync::Lazy;
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};

pub const CHUNK_SIZE: u64 = 1 << 26;
pub const BLOCK_SIZE: u64 = 1 << 22;
pub const FS_BLK_SIZE: u64 = 128 << 20;
pub const FS_FUSE_MAX_IO_SIZE: u64 = 128u64 << 10;
pub const FS_TOTAL_INODES: u64 = 1 << 20;
pub const FS_META_CACHE_SIZE: usize = 16384;

pub const FS_PAGE_SIZE: u64 = 4096;

pub const FS_ROOT_INODE: u64 = 1;

static mut DATA_PATH: Lazy<String> = Lazy::new(|| "".to_string());
pub const fn is_power_of2(size: u64) -> bool {
    (size > 0) && (size & (size - 1)) == 0
}

pub const fn align_up(size: u64, align: u64) -> u64 {
    (size + (align - 1)) & !(align - 1)
}

pub fn init_data_path(mp: &str) {
    unsafe {
        *DATA_PATH = mp.to_string();
    }
}

pub fn get_data_path() -> &'static String {
    unsafe { &DATA_PATH }
}

pub fn to_systime(s: u64) -> SystemTime {
    UNIX_EPOCH + time::Duration::from_secs(s)
}

pub fn to_filetype(s: Itype) -> FileType {
    match s {
        Itype::File => FileType::RegularFile,
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
