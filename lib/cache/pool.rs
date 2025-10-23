use crate::utils::{align_up, BitMap, FS_PAGE_SIZE};
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

static mut G_MEMPOOL: NonNull<MemPool> = NonNull::dangling();

pub struct MemPool {
    ptr: *mut u8,
    cap: usize,
    dmap: BitMap,
}

impl MemPool {
    pub fn init(cap: u64) {
        unsafe {
            let obj = Box::new(MemPool::new(cap));
            let ptr = Box::into_raw(obj);
            G_MEMPOOL = NonNull::new(ptr).expect("can't create nonnull");
        }
    }

    // unnecessary for long-running program
    pub fn destroy() {
        unsafe {
            let ptr = G_MEMPOOL.as_ptr();
            let _ = Box::from_raw(ptr);
        }
    }

    #[allow(static_mut_refs)]
    pub fn get() -> &'static mut MemPool {
        unsafe { G_MEMPOOL.as_mut() }
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
        unsafe {
            let off = ptr.offset_from(self.ptr) as u64;
            let bit = off / FS_PAGE_SIZE;
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

        let x = MemPool::get().alloc();
        let y = MemPool::get().alloc();
        let z = MemPool::get().alloc();

        unsafe {
            let sz = FS_PAGE_SIZE as isize;
            assert_eq!(y.offset_from(x), sz);
            assert_eq!(z.offset_from(y), sz);
        }
        assert!(MemPool::get().full());

        MemPool::destroy();
    }
}
