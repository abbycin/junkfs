use crate::utils::{align_up, BitMap, FS_PAGE_SIZE};
use once_cell::sync::Lazy;
use std::mem::ManuallyDrop;
use std::sync::Mutex;

pub struct MemPool {
    ptr: *mut u8,
    cap: usize,
    dmap: BitMap,
}

unsafe impl Send for MemPool {}
unsafe impl Sync for MemPool {}

static G_MEMPOOL: Lazy<Mutex<Option<MemPool>>> = Lazy::new(|| Mutex::new(None));

impl MemPool {
    pub fn init(cap: u64) {
        let mut pool = G_MEMPOOL.lock().unwrap();
        *pool = Some(MemPool::new(cap));
    }

    pub fn destroy() {
        let mut pool = G_MEMPOOL.lock().unwrap();
        *pool = None;
    }

    pub fn alloc_block() -> *mut u8 {
        let mut pool = G_MEMPOOL.lock().unwrap();
        if let Some(ref mut p) = *pool {
            p.alloc()
        } else {
            std::ptr::null_mut()
        }
    }

    pub fn free_block(ptr: *mut u8) {
        let mut pool = G_MEMPOOL.lock().unwrap();
        if let Some(ref mut p) = *pool {
            p.free(ptr);
        }
    }

    pub fn is_full() -> bool {
        let pool = G_MEMPOOL.lock().unwrap();
        if let Some(ref p) = *pool {
            p.full()
        } else {
            true
        }
    }

    fn new(cap: u64) -> Self {
        let cap = align_up(cap, FS_PAGE_SIZE);
        let mut v = ManuallyDrop::new(vec![0u8; cap as usize]);
        Self {
            ptr: v.as_mut_ptr(),
            cap: cap as usize,
            dmap: BitMap::new(cap / FS_PAGE_SIZE),
        }
    }

    pub fn alloc(&mut self) -> *mut u8 {
        if let Some(x) = self.dmap.alloc() {
            unsafe { self.ptr.add((x * FS_PAGE_SIZE) as usize) }
        } else {
            std::ptr::null_mut()
        }
    }

    pub fn free(&mut self, ptr: *mut u8) {
        if ptr.is_null() {
            log::error!("mempool free null ptr");
            panic!("mempool free null");
        }
        unsafe {
            let off = ptr.offset_from(self.ptr) as u64;
            if off >= self.cap as u64 || off % FS_PAGE_SIZE != 0 {
                log::error!("mempool free invalid ptr {:?} off {} cap {}", ptr, off, self.cap);
                panic!("mempool free invalid ptr");
            }
            let bit = off / FS_PAGE_SIZE;
            if !self.dmap.test(bit) {
                log::error!("mempool double free ptr {:?} bit {}", ptr, bit);
                panic!("mempool double free");
            }
            self.dmap.del(bit);
        }
    }

    pub fn full(&self) -> bool {
        self.dmap.full()
    }
}

impl Drop for MemPool {
    fn drop(&mut self) {
        unsafe {
            assert!(!self.ptr.is_null());
            let _ = Vec::from_raw_parts(self.ptr, self.cap, self.cap);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::cache::MemPool;
    use crate::utils::FS_PAGE_SIZE;

    #[test]
    fn test_pool() {
        let mut p = MemPool::new(32 * FS_PAGE_SIZE);
        let mut v = Vec::new();

        while !p.full() {
            v.push(p.alloc());
        }

        assert_eq!(v.len(), 32);
        for i in &v {
            p.free(*i);
        }

        assert!(!p.full());
    }

    #[test]
    fn test_singleton() {
        MemPool::init(FS_PAGE_SIZE * 3);

        let x = MemPool::alloc_block();
        let y = MemPool::alloc_block();
        let z = MemPool::alloc_block();

        unsafe {
            let sz = FS_PAGE_SIZE as isize;
            assert_eq!(y.offset_from(x), sz);
            assert_eq!(z.offset_from(y), sz);
        }
        assert!(MemPool::is_full());

        MemPool::free_block(x);
        MemPool::free_block(y);
        MemPool::free_block(z);

        MemPool::destroy();
    }
}
