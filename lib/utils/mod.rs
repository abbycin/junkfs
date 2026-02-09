mod bitmap;
mod bitmap64;

pub use bitmap::BitMap;
pub use bitmap64::BitMap64;
use once_cell::sync::Lazy;

pub const CHUNK_SIZE: u64 = 1 << 26;
pub const BLOCK_SIZE: u64 = 1 << 22;
pub const FS_BLK_SIZE: u64 = 128 << 10;
pub const FS_FUSE_MAX_IO_SIZE: u64 = 128u64 << 10;
pub const FS_TOTAL_INODES: u64 = 2 << 20;
pub const FS_IMAP_GROUP_SIZE: u64 = 4096;

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

#[allow(static_mut_refs)]
pub fn get_data_path() -> &'static String {
    unsafe { &DATA_PATH }
}
