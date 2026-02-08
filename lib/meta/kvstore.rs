use crate::cache::Flusher;
use mace::{Bucket, Mace, OpCode, Options, TxnKV, TxnView};

pub struct MaceStore {
    _db: Mace,
    bucket: Bucket,
}

impl Flusher<String, Vec<u8>> for MaceStore {
    fn flush(&mut self, key: String, data: Vec<u8>) {
        let kv = self.bucket.begin().expect("can't fail");
        kv.upsert(&key, &data).unwrap();
        kv.commit().unwrap();
    }
}

impl MaceStore {
    pub fn new(meta_path: &str) -> Self {
        let mut opt = Options::new(meta_path);
        opt.concurrent_write = 4;
        opt.wal_file_size = 32 << 20;
        opt.max_log_size = 64 << 20;
        opt.gc_eager = true;
        opt.data_garbage_ratio = 10;
        opt.gc_timeout = 10000; // 10s

        let db = Mace::new(opt.validate().unwrap()).unwrap();
        let bucket = Self::open_bucket(&db).unwrap();
        Self { _db: db, bucket }
    }

    pub fn insert(&self, key: &str, val: &[u8]) -> Result<(), OpCode> {
        let kv = self.bucket.begin()?;
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

    pub fn begin(&self) -> Result<TxnKV<'_>, OpCode> {
        self.bucket.begin()
    }

    pub fn get(&self, key: &str) -> Result<Vec<u8>, OpCode> {
        let view = self.bucket.view()?;
        let x = view.get(key);
        match x {
            Err(e) => {
                log::error!("get {} fail, error {:?}", key, e);
                Err(e)
            }
            Ok(o) => Ok(o.slice().to_vec()),
        }
    }

    pub fn get_optional(&self, key: &str) -> Result<Option<Vec<u8>>, OpCode> {
        let view = self.bucket.view()?;
        let x = view.get(key);
        match x {
            Err(OpCode::NotFound) => Ok(None),
            Err(e) => {
                log::error!("get {} fail, error {:?}", key, e);
                Err(e)
            }
            Ok(o) => Ok(Some(o.slice().to_vec())),
        }
    }

    pub fn view(&self) -> TxnView<'_> {
        self.bucket.view().expect("can't fail")
    }

    pub fn remove(&self, key: &str) -> Result<(), OpCode> {
        let kv = self.bucket.begin()?;
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
        let view = self.bucket.view()?;
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

    pub fn sync(&self) -> Result<(), OpCode> {
        self._db.sync()
    }

    pub(crate) fn open_bucket(db: &Mace) -> Result<Bucket, OpCode> {
        const META_BUCKET: &str = "junkfs_meta";
        match db.get_bucket(META_BUCKET) {
            Ok(b) => Ok(b),
            Err(OpCode::NotFound) => db.new_bucket(META_BUCKET),
            Err(e) => Err(e),
        }
    }
}
