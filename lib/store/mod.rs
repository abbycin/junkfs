mod cache_store;
mod filestore;

use crate::meta::{Ino, Meta};
pub use cache_store::CacheStore;
pub use filestore::FileStore;

#[allow(dead_code)]
#[derive(Debug)]
struct Entry {
    blk_id: u64,   // block id
    blk_off: u64,  // offset in block
    off: u64,      // global offset in file
    size: u64,     // data length
    data: *mut u8, // data buffer
}

unsafe impl Send for Entry {}
unsafe impl Sync for Entry {}

trait Store: Send {
    fn write(&mut self, meta: &mut Meta, ino: Ino, buf: &[Entry]);

    fn read(&mut self, meta: &mut Meta, ino: Ino, off: u64, size: usize) -> Option<Vec<u8>>;
}
