use crate::cache::{Flusher, LRUCache};
use crate::meta::meta_store::{MetaIter, MetaStore};
use sled::IVec;
use std::cell::RefCell;

pub struct SledStore {
    /// read cache
    cache: RefCell<LRUCache<String, Vec<u8>>>,
    db: sled::Db,
}

fn transform_iter<T: Iterator<Item = Result<(IVec, IVec), sled::Error>>>(
    iter: T,
) -> impl Iterator<Item = Option<Vec<u8>>> {
    iter.map(|x| match x {
        Err(e) => {
            log::warn!("transform iter {}", e.to_string());
            None
        }
        Ok((_, v)) => Some(v.to_vec()),
    })
}

impl Flusher<String, Vec<u8>> for SledStore {
    fn flush(&mut self, key: String, data: Vec<u8>) {
        match self.db.insert(&key, data.as_slice()) {
            Err(e) => {
                log::error!("can't store key {} error {}", key, e.to_string());
            }
            Ok(_) => {}
        }
    }
}

impl SledStore {
    pub fn new(meta_path: &str, cache_cap: usize) -> Self {
        let s = Self {
            cache: RefCell::new(LRUCache::new(cache_cap)),
            db: sled::open(meta_path).unwrap(),
        };
        // unnecessary to flush, use default dummy backend
        // let p = std::ptr::addr_of_mut!(s);
        // s.cache.borrow_mut().set_backend(p);
        s
    }
}

impl MetaStore for SledStore {
    fn insert(&self, key: &str, val: &[u8]) -> Result<(), String> {
        match self.db.insert(key, val) {
            Err(e) => {
                log::error!("insert {} fail, error {}", key, e);
                Err(e.to_string())
            }
            Ok(_) => {
                self.cache.borrow_mut().add(key.to_string(), val.to_vec());
                Ok(())
            }
        }
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        if let Some(v) = self.cache.borrow_mut().get(&key.to_string()) {
            return Ok(Some(v.clone()));
        }
        match self.db.get(key) {
            Err(e) => {
                log::error!("get {} fail, error {}", key, e);
                Err(e.to_string())
            }
            Ok(o) => match o {
                None => Ok(None),
                Some(o) => {
                    self.cache.borrow_mut().add(key.to_string(), o.to_vec());
                    Ok(Some(o.to_vec()))
                }
            },
        }
    }

    fn scan_prefix(&self, prefix: &str) -> MetaIter {
        let iter = self.db.scan_prefix(prefix);

        MetaIter {
            iter: Box::new(transform_iter(iter)),
        }
    }

    fn remove(&self, key: &str) -> Result<(), String> {
        self.cache.borrow_mut().del(&key.to_string());
        match self.db.remove(key) {
            Err(e) => {
                log::error!("remove {} fail, error {}", key, e);
                Err(e.to_string())
            }
            Ok(_) => Ok(()),
        }
    }

    fn contains_key(&self, key: &str) -> Result<bool, String> {
        if let Some(_) = self.cache.borrow_mut().get(&key.to_string()) {
            return Ok(true);
        }
        match self.db.contains_key(key) {
            Err(e) => {
                log::error!("contains_key {} fail, error {}", key, e);
                Err(e.to_string())
            }
            Ok(o) => Ok(o),
        }
    }

    fn flush(&self) {
        // if backend is set to sled, sled will SIGSEGV on `insert`
        // self.cache.borrow_mut().flush();
        let _r = self.db.flush();
    }
}

impl Drop for SledStore {
    fn drop(&mut self) {
        self.cache.borrow_mut().flush();
        let _ = self.db.flush();
    }
}
