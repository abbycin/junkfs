mod lru;
mod pool;

pub use pool::MemPool;

pub use lru::LRUCache;

pub trait Flusher<K, V> {
    fn flush(&mut self, key: K, data: V);
}
