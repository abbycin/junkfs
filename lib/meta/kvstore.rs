use crate::cache::Flusher;
use mace::{Iter, Mace, OpCode, Options};

pub struct MaceStore {
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
    pub fn new(meta_path: &str) -> Self {
        let mut opt = Options::new(meta_path);
        opt.workers = 4; // we will lookup while iterating prefix, so 1 worker is not enough
        opt.wal_file_size = 16 << 20;
        opt.max_log_size = 24 << 20;
        opt.gc_eager = true;
        opt.data_garbage_ratio = 10;
        opt.gc_timeout = 10000; // 10s

        Self {
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
            Ok(_) => Ok(()),
        }
    }

    pub fn get(&self, key: &str) -> Result<Vec<u8>, OpCode> {
        let view = self.db.view()?;
        let x = view.get(key);
        match x {
            Err(e) => {
                log::error!("get {} fail, error {:?}", key, e);
                Err(e)
            }
            Ok(o) => Ok(o.to_vec()),
        }
    }

    pub fn scan_prefix(&'_ self, prefix: &str) -> Iter<'_> {
        let view = self.db.view().unwrap();
        view.seek(prefix)
    }

    pub fn remove(&self, key: &str) -> Result<(), OpCode> {
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
