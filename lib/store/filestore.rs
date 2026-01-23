use crate::cache::{Flusher, LRUCache};
use crate::meta::{Ino, Meta};
use crate::store::{Entry, Store};
use crate::utils::{get_data_path, FS_BLK_SIZE, FS_FUSE_MAX_IO_SIZE};
use once_cell::sync::Lazy;
use std::cmp::{max, min};
use std::os::unix::prelude::FileExt;
use std::sync::Mutex;
const MAX_CACHE_ITEMS: usize = 256;

struct FileFlusher;

static mut G_LUSHER: FileFlusher = FileFlusher;

impl Flusher<String, std::fs::File> for FileFlusher {
    fn flush(&mut self, key: String, data: std::fs::File) {
        let file = data;
        file.sync_all().unwrap_or_else(|_| panic!("can't sync file {}", key));
        drop(file);
    }
}

static G_FILE_CACHE: Lazy<Mutex<LRUCache<String, std::fs::File>>> = Lazy::new(|| {
    let mut c = LRUCache::new(MAX_CACHE_ITEMS);
    let p = std::ptr::addr_of_mut!(G_LUSHER);
    c.set_backend(p);
    Mutex::new(c)
});

pub struct FileStore;

impl Flusher<u64, std::fs::File> for FileStore {
    fn flush(&mut self, key: u64, data: std::fs::File) {
        log::warn!("close file {}", key);
        drop(data);
    }
}

impl FileStore {
    fn read_key(ino: Ino, blk: u64) -> String {
        format!("{}r{}", ino, blk)
    }

    fn write_key(ino: Ino, blk: u64) -> String {
        format!("{}w{}", ino, blk)
    }

    fn build_path(ino: Ino, blk: u64) -> String {
        format!("{}/{}/{}", get_data_path(), ino, blk)
    }

    fn build_dir(ino: Ino) -> String {
        format!("{}/{}", get_data_path(), ino)
    }

    pub fn unlink(ino: Ino, blk_id: u64) {
        let p = Self::build_path(ino, blk_id);
        match std::fs::remove_file(&p) {
            Err(e) => {
                log::error!("can't remove {} error {}", p, e);
            }
            Ok(_) => {
                log::info!("remove file {}", p);
            }
        }
    }

    fn get_fp_and_then<F, R>(key: String, ino: Ino, blk: u64, f: F) -> Option<R>
    where
        F: FnOnce(&mut std::fs::File) -> R,
    {
        let mut cache = G_FILE_CACHE.lock().unwrap();
        if let Some(fp) = cache.get_mut(&key) {
            Some(f(fp))
        } else {
            let _ = std::fs::create_dir_all(Self::build_dir(ino));
            let fpath = Self::build_path(ino, blk);
            let fp = std::fs::File::options()
                .read(true)
                .write(true)
                .truncate(false)
                .create(true)
                .open(&fpath);
            if let Ok(fp) = fp {
                let fp_ref = cache.add(key, fp).unwrap();
                Some(f(fp_ref))
            } else {
                log::error!("can't create {}", fpath);
                None
            }
        }
    }

    fn write_impl(&mut self, ino: Ino, e: &Entry) -> bool {
        let key = Self::write_key(ino, e.blk_id);
        Self::get_fp_and_then(key, ino, e.blk_id, |fp| unsafe {
            let s = std::slice::from_raw_parts(e.data, e.size as usize);
            let r = fp.write_at(s, e.blk_off);
            if let Err(err) = r {
                log::error!("can't write entry {:?} error {}", e, err);
                false
            } else {
                true
            }
        })
        .unwrap_or(false)
    }

    fn read_impl(&mut self, ino: Ino, off: u64, size: usize) -> Option<Vec<u8>> {
        let blk_id = off / FS_BLK_SIZE;
        let key = Self::read_key(ino, blk_id);

        let mut sz = min(FS_FUSE_MAX_IO_SIZE, size as u64);
        if (off + sz) / FS_BLK_SIZE == (blk_id + 1) {
            sz = (blk_id + 1) * FS_BLK_SIZE - off;
        }

        Self::get_fp_and_then(key, ino, blk_id, |fp| {
            let mut v = vec![0u8; sz as usize];
            match fp.read_at(&mut v, off % FS_BLK_SIZE) {
                Ok(n) => {
                    v.truncate(n);
                    Some(v)
                }
                Err(e) => {
                    log::error!(
                        "can't read data blk_id {} off {} size {} error {}",
                        blk_id,
                        off % FS_BLK_SIZE,
                        sz,
                        e
                    );
                    None
                }
            }
        })
        .flatten()
    }
}

impl Store for FileStore {
    fn write(&mut self, meta: &mut Meta, ino: Ino, buf: &[Entry]) {
        if buf.is_empty() {
            return;
        }
        let mut sz = 0;
        let mut inode = meta.load_inode(ino).expect("can't load inode");

        let mut affected_blks = std::collections::HashSet::new();

        for e in buf {
            sz = max(sz, e.off + e.size);
            if !self.write_impl(ino, e) {
                log::warn!("write {}_{} fail", ino, e.blk_id);
                return;
            }
            affected_blks.insert(e.blk_id);
        }

        // ensure data is on disk before updating metadata
        for blk_id in affected_blks {
            let key = Self::write_key(ino, blk_id);
            let res = Self::get_fp_and_then(key, ino, blk_id, |fp| fp.sync_all().map_err(|e| e.to_string()));
            if let Some(Err(e)) = res {
                log::error!("can't sync file {}_{} error {}", ino, blk_id, e);
            }
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        inode.mtime = now;
        inode.ctime = now;

        // try update inode.length
        if inode.length < sz {
            log::info!("trying to update inode.length {} to {}", inode.length, sz);
            inode.length = sz;
        }
        meta.store_inode(&inode).unwrap()
    }

    fn read(&mut self, meta: &mut Meta, ino: Ino, off: u64, size: usize) -> Option<Vec<u8>> {
        let inode = meta.load_inode(ino).expect("can't load inode");
        if off >= inode.length {
            return Some(Vec::new());
        }
        let size = min(size as u64, inode.length - off) as usize;
        self.read_impl(ino, off, size)
    }
}

