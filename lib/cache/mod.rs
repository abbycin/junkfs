mod lru;
mod pool;

pub use pool::MemPool;

pub use lru::LRUCache;

pub trait Store<T> {
    fn store(&mut self, data: &T);
}
