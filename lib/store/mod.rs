mod cache_store;
mod filestore;

use crate::meta::{Ino, Meta};
pub use cache_store::CacheStore;
pub use filestore::FileStore;

#[derive(Debug)]
struct Entry {
    blk_id: u64,   // block id
    blk_off: u64,  // offset in block
    off: u64,      // global offset in file
    size: u64,     // data length
    data: *mut u8, // data buffer
}

trait Store {
    fn write(&mut self, meta: &mut Meta, ino: Ino, buf: &Vec<Entry>);

    fn read(&mut self, ino: Ino, off: u64, size: usize) -> Option<Vec<u8>>;
}
