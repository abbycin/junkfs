mod lru;
mod pool;

pub use pool::MemPool;

pub use lru::LRUCache;

pub trait Store<K, V> {
    fn store(&mut self, key: K, data: V);
}
