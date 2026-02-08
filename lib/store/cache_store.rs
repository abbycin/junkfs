use crate::cache::MemPool;
use crate::meta::Ino;
use crate::store::{record_flush, record_write, Entry, FileStore};
use crate::utils::{FS_BLK_SIZE, FS_PAGE_SIZE};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const CACHE_LIMIT: usize = 1024; // cache limit in pages
const FLUSH_INTERVAL: Duration = Duration::from_millis(200);
const FLUSH_BYTES: usize = CACHE_LIMIT * FS_PAGE_SIZE as usize;

pub struct CacheStore {
    ino: Ino,
    bufs: Vec<Entry>,
    page_map: HashMap<u64, usize>,
    dirty_bytes: usize,
    last_write: Instant,
}

impl CacheStore {
    pub fn new(ino: Ino) -> Self {
        Self {
            ino,
            bufs: Vec::new(),
            page_map: HashMap::new(),
            dirty_bytes: 0,
            last_write: Instant::now(),
        }
    }

    pub fn ino(&self) -> Ino {
        self.ino
    }

    /// `off` is global file offset, we need map to block_id and block offset
    /// NOTE: the data maybe cross blocks, we need split into two blocks
    pub fn write(&mut self, off: u64, data: &[u8]) -> usize {
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
            let n = self.write_block(blk1, blk_off1, off1, data1);
            if n != data1.len() {
                nbytes += n;
                return nbytes;
            }

            let data2 = &data[rest_bytes as usize..];
            let blk2 = blk1 + 1;
            let blk_off2 = 0;
            let off2 = blk2 * FS_BLK_SIZE;
            assert_eq!(blk_off2 * FS_BLK_SIZE % FS_BLK_SIZE, off2);
            let n = self.write_block(blk2, blk_off2, off2, data2);
            if n != data2.len() {
                nbytes += n;
                return nbytes;
            }
        } else {
            nbytes += self.write_block(blk, pos, off, data);
        }
        record_write(nbytes);
        self.last_write = Instant::now();
        nbytes
    }

    pub fn read(&self, off: u64, size: usize) -> Option<Vec<u8>> {
        if size == 0 {
            return Some(Vec::new());
        }
        let mut data = FileStore::read_at(self.ino, off, size).unwrap_or_else(|| vec![0u8; size]);
        if data.len() < size {
            data.resize(size, 0);
        }
        let start = off;
        let end = off + size as u64;
        for e in &self.bufs {
            let e_start = e.off;
            let e_end = e.off + e.size;
            if e_end <= start || e_start >= end {
                continue;
            }
            let copy_start = max(e_start, start);
            let copy_end = min(e_end, end);
            let dst_off = (copy_start - start) as usize;
            let src_off = (copy_start - e_start) as usize;
            let len = (copy_end - copy_start) as usize;
            unsafe {
                std::ptr::copy_nonoverlapping(e.data.add(src_off), data.as_mut_ptr().add(dst_off), len);
            }
        }
        Some(data)
    }

    pub fn clear(&mut self) {
        for i in &self.bufs {
            MemPool::free_block(i.data);
        }
        self.bufs.clear();
        self.page_map.clear();
        self.dirty_bytes = 0;
    }

    pub fn should_flush(&self) -> bool {
        !self.bufs.is_empty() && (self.dirty_bytes >= FLUSH_BYTES || self.last_write.elapsed() >= FLUSH_INTERVAL)
    }

    pub(crate) fn take_entries(&mut self) -> Vec<Entry> {
        let mut bufs = Vec::new();
        std::mem::swap(&mut self.bufs, &mut bufs);
        self.page_map.clear();
        self.dirty_bytes = 0;
        bufs
    }

    pub(crate) fn flush_entries(ino: Ino, bufs: Vec<Entry>, sync: bool) -> Result<(), String> {
        if bufs.is_empty() {
            return Ok(());
        }
        let mut bytes = 0u64;
        for e in &bufs {
            bytes += e.size;
        }
        let start = Instant::now();
        let r = FileStore::write_entries(ino, &bufs, sync);
        let ns = start.elapsed().as_nanos() as u64;
        for i in &bufs {
            MemPool::free_block(i.data);
        }
        record_flush(bytes, ns, r.is_ok());
        r
    }

    pub fn flush(&mut self, sync: bool) -> Result<(), String> {
        let bufs = self.take_entries();
        Self::flush_entries(self.ino, bufs, sync)
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

    fn write_block(&mut self, blk_id: u64, blk_off: u64, off: u64, data: &[u8]) -> usize {
        let base = data.as_ptr();
        let end = unsafe { base.add(data.len()) };
        let len = data.len();
        let mut i = 0;
        let mut nbytes = 0;

        while i < len {
            let sz = min(len - i, FS_PAGE_SIZE as usize);
            let page_off = off + i as u64;
            if sz as u64 == FS_PAGE_SIZE && page_off % FS_PAGE_SIZE == 0 {
                if let Some(&idx) = self.page_map.get(&page_off) {
                    let dst = self.bufs[idx].data;
                    unsafe {
                        let ptr = base.add(i);
                        assert!(ptr < end);
                        std::ptr::copy(ptr, dst, sz);
                    }
                    i += sz;
                    nbytes += sz;
                    continue;
                }
            }
            let mem = self.alloc();
            if mem.is_null() {
                return nbytes;
            }
            assert!(sz <= FS_PAGE_SIZE as usize);
            unsafe {
                let ptr = base.add(i);
                assert!(ptr < end);
                self.copy_data(ptr, mem, sz, blk_id, blk_off + i as u64, off + i as u64);
            }
            if sz as u64 == FS_PAGE_SIZE && page_off % FS_PAGE_SIZE == 0 {
                self.page_map.insert(page_off, self.bufs.len() - 1);
            }
            self.dirty_bytes += sz;
            i += sz;
            nbytes += sz;
        }
        nbytes
    }

    fn alloc(&mut self) -> *mut u8 {
        if self.bufs.len() >= CACHE_LIMIT || MemPool::is_full() {
            let _ = self.flush(false);
        }
        MemPool::alloc_block()
    }
}
