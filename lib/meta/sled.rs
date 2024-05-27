use crate::cache::{LRUCache, Store};
use crate::meta::meta_store::{MetaIter, MetaStore};
use crate::meta::{Ino, MetaItem};
use sled::IVec;

pub struct SledStore {
    cache: LRUCache<Ino, Box<dyn MetaItem>>,
    db: sled::Db,
}

fn tranform_iter<T: Iterator<Item = Result<(IVec, IVec), sled::Error>>>(
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

impl SledStore {
    pub fn new(meta_path: &str, cache_cap: usize) -> Self {
        let mut s = Self {
            cache: LRUCache::new(cache_cap),
            db: sled::open(meta_path).unwrap(),
        };
        let p = std::ptr::addr_of_mut!(s);
        s.cache.set_backend(p);
        s
    }
}

impl Store<Box<dyn MetaItem>> for SledStore {
    fn store(&mut self, data: &Box<dyn MetaItem>) {
        let key = data.key();
        let r = self.insert(&key, &data.val());
        if r.is_err() {
            log::error!("flush key {} error {}", key, r.err().unwrap().to_string());
        }
    }
}

impl MetaStore for SledStore {
    fn insert(&self, key: &str, val: &[u8]) -> Result<(), String> {
        match self.db.insert(key, val) {
            Err(e) => Err(e.to_string()),
            Ok(_) => Ok(()),
        }
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        match self.db.get(key) {
            Err(e) => Err(e.to_string()),
            Ok(o) => match o {
                None => Ok(None),
                Some(o) => Ok(Some(o.to_vec())),
            },
        }
    }

    fn scan_prefix(&self, prefix: &str) -> MetaIter {
        let iter = self.db.scan_prefix(prefix);

        MetaIter {
            iter: Box::new(tranform_iter(iter)),
        }
    }

    fn remove(&self, key: &str) -> Result<(), String> {
        match self.db.remove(key) {
            Err(e) => Err(e.to_string()),
            Ok(_) => Ok(()),
        }
    }

    fn contains_key(&self, key: &str) -> Result<bool, String> {
        match self.db.contains_key(key) {
            Err(e) => Err(e.to_string()),
            Ok(o) => Ok(o),
        }
    }

    fn flush(&mut self) {
        self.cache.flush();
        let _r = self.db.flush();
    }
}
