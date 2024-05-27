use crate::cache::MemPool;
use crate::meta::{Ino, Meta};
use crate::store::{Entry, FileStore, Store};
use crate::utils::{FS_BLK_SIZE, FS_PAGE_SIZE};
use std::cmp::min;

const CACHE_LIMIT: usize = 32; // 128K

pub struct CacheStore {
    ino: Ino,
    bufs: Vec<Entry>,
    store: Box<dyn Store>,
}

impl CacheStore {
    pub fn new(ino: Ino) -> Self {
        Self {
            ino,
            bufs: Vec::new(),
            store: Box::new(FileStore::new()),
        }
    }

    /// `off` is global file offset, we need map to block_id and block offset
    /// NOTE: the data maybe cross blocks, we need split into two blocks
    pub fn write(&mut self, meta: &mut Meta, off: u64, data: &[u8]) -> usize {
        assert!(data.len() <= FS_BLK_SIZE as usize);
        let pos = off % FS_BLK_SIZE;
        let blk = off / FS_BLK_SIZE;
        let rest_bytes = FS_BLK_SIZE - pos;
        let len = data.len() as u64;
        let mut nbytes = 0;

        // require two blocks
        if len > rest_bytes {
            let data1 = &data[0..rest_bytes as usize];
            let blk1 = blk;
            let blk_off1 = pos;
            let off1 = off;
            let n = self.write_block(meta, blk1, blk_off1, off1, data1);
            if n != data1.len() {
                nbytes += n;
                return nbytes;
            }

            let data2 = &data[rest_bytes as usize..];
            let blk2 = blk1 + 1;
            let blk_off2 = 0;
            let off2 = blk2 * FS_BLK_SIZE;
            assert_eq!(blk_off2 * FS_BLK_SIZE % FS_BLK_SIZE, off2);
            let n = self.write_block(meta, blk2, blk_off2, off2, data2);
            if n != data2.len() {
                nbytes += n;
                return nbytes;
            }
        } else {
            nbytes += self.write_block(meta, blk, pos, off, data);
        }
        nbytes
    }

    pub fn read(&mut self, meta: &mut Meta, off: u64, size: usize) -> Option<Vec<u8>> {
        self.flush(meta);
        self.store.read(self.ino, off, size)
    }

    fn copy_data(&mut self, src: *const u8, dst: *mut u8, size: usize, blk_id: u64, blk_off: u64, off: u64) {
        unsafe {
            std::ptr::copy(src, dst, size);
        }
        let e = Entry {
            blk_id,
            blk_off,
            off,
            size: size as u64,
            data: dst,
        };
        self.bufs.push(e);
    }

    fn write_block(&mut self, meta: &mut Meta, blk_id: u64, blk_off: u64, off: u64, data: &[u8]) -> usize {
        let mut ptr = data.as_ptr();
        let end = unsafe { ptr.add(data.len()) };
        let len = data.len();
        let mut i = 0;
        let mut nbytes = 0;

        while i < len {
            let sz = min(len - i, FS_PAGE_SIZE as usize);
            let mem = self.alloc(meta);
            if mem.is_null() {
                return nbytes;
            }
            unsafe {
                ptr = ptr.add(i as usize);
                assert!(ptr < end);
            }
            assert!(sz <= FS_PAGE_SIZE as usize);
            self.copy_data(ptr, mem, sz, blk_id, blk_off + i as u64, off);
            i += sz;
            nbytes += sz;
        }
        nbytes
    }

    fn alloc(&mut self, meta: &mut Meta) -> *mut u8 {
        if self.bufs.len() >= CACHE_LIMIT || MemPool::get().full() {
            log::info!("flush cache");
            self.flush(meta);
        }
        return MemPool::get().alloc();
    }

    // NOTE: the entry's order is mattered in bufs, do NOT reorder them
    pub fn flush(&mut self, meta: &mut Meta) {
        self.store.write(meta, self.ino, &self.bufs);
        for i in &self.bufs {
            MemPool::get().free(i.data);
        }
        self.bufs.clear();
    }
}
