use crate::cache::{Flusher, LRUCache};
use mace::{Iter, Mace, OpCode, Options};
use std::cell::RefCell;

pub struct MaceStore {
    /// read cache
    cache: RefCell<LRUCache<String, Vec<u8>>>,
    db: Mace,
}

impl Flusher<String, Vec<u8>> for MaceStore {
    fn flush(&mut self, key: String, data: Vec<u8>) {
        let kv = self.db.begin().unwrap();
        kv.upsert(&key, &data).unwrap();
        kv.commit().unwrap();
    }
}

impl MaceStore {
    pub fn new(meta_path: &str, cache_cap: usize) -> Self {
        let mut opt = Options::new(meta_path);
        opt.workers = 1;
        opt.wal_file_size = 16 << 20;
        opt.max_log_size = 64 << 20;
        opt.gc_eager = true;
        opt.gc_ratio = 10;
        opt.gc_timeout = 10000; // 10s

        Self {
            cache: RefCell::new(LRUCache::new(cache_cap)),
            db: Mace::new(opt.validate().unwrap()).unwrap(),
        }
    }

    pub fn insert(&self, key: &str, val: &[u8]) -> Result<(), OpCode> {
        let kv = self.db.begin()?;
        kv.upsert(key, val)?;
        let e = kv.commit();
        match e {
            Err(e) => {
                log::error!("insert {} fail, error {:?}", key, e);
                Err(e)
            }
            Ok(_) => {
                self.cache.borrow_mut().add(key.to_string(), val.to_vec());
                Ok(())
            }
        }
    }

    pub fn get(&self, key: &str) -> Result<Vec<u8>, OpCode> {
        if let Some(v) = self.cache.borrow_mut().get(&key.to_string()) {
            return Ok(v.clone());
        }
        let view = self.db.view()?;
        let x = view.get(key);
        match x {
            Err(e) => {
                log::error!("get {} fail, error {:?}", key, e);
                Err(e)
            }
            Ok(o) => {
                let v = o.to_vec();
                self.cache.borrow_mut().add(key.to_string(), v.clone());
                Ok(v)
            }
        }
    }

    pub fn scan_prefix(&'_ self, prefix: &str) -> Iter<'_> {
        let view = self.db.view().unwrap();
        view.seek(prefix)
    }

    pub fn remove(&self, key: &str) -> Result<(), OpCode> {
        self.cache.borrow_mut().del(&key.to_string());
        let kv = self.db.begin()?;
        kv.del(key)?;
        let x = kv.commit();
        match x {
            Err(e) => {
                log::error!("remove {} fail, error {:?}", key, e);
                Err(e)
            }
            Ok(_) => Ok(()),
        }
    }

    pub fn contains_key(&self, key: &str) -> Result<bool, OpCode> {
        if self.cache.borrow_mut().get(&key.to_string()).is_some() {
            return Ok(true);
        }
        let view = self.db.view()?;
        let x = view.get(key);
        match x {
            Err(OpCode::NotFound) => Ok(false),
            Err(e) => {
                log::error!("contains_key {} fail, error {:?}", key, e);
                Err(e)
            }
            Ok(_) => Ok(true),
        }
    }
}

impl Drop for MaceStore {
    fn drop(&mut self) {
        self.cache.borrow_mut().flush();
    }
}
