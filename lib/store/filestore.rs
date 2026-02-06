use crate::cache::{Flusher, LRUCache};
use crate::meta::{Ino, Meta};
use crate::store::{Entry, Store};
use crate::utils::{get_data_path, FS_FUSE_MAX_IO_SIZE};
use once_cell::sync::Lazy;
use std::cmp::{max, min};
use std::io::ErrorKind;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::Mutex;
const MAX_CACHE_ITEMS: usize = 256;
const DATA_SHARD_BITS: u64 = 8;
const DATA_SHARD_MASK: u64 = (1 << DATA_SHARD_BITS) - 1;

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
    fn file_key(ino: Ino) -> String {
        format!("i{}", ino)
    }

    fn shard(ino: Ino) -> (u8, u8) {
        let s1 = (ino & DATA_SHARD_MASK) as u8;
        let s2 = ((ino >> DATA_SHARD_BITS) & DATA_SHARD_MASK) as u8;
        (s1, s2)
    }

    fn build_dir1(ino: Ino) -> String {
        let (s1, _) = Self::shard(ino);
        format!("{}/{:02x}", get_data_path(), s1)
    }

    fn build_dir(ino: Ino) -> String {
        let (s1, s2) = Self::shard(ino);
        format!("{}/{:02x}/{:02x}", get_data_path(), s1, s2)
    }

    fn build_path(ino: Ino) -> String {
        let (s1, s2) = Self::shard(ino);
        format!("{}/{:02x}/{:02x}/{}", get_data_path(), s1, s2, ino)
    }

    fn fsync_dir(path: &str) {
        match std::fs::File::open(path) {
            Ok(dir) => {
                if let Err(e) = dir.sync_all() {
                    log::error!("can't sync dir {} error {}", path, e);
                }
            }
            Err(e) => {
                log::error!("can't open dir {} error {}", path, e);
            }
        }
    }

    fn ensure_root_dir() -> Result<(), String> {
        let root = get_data_path().as_str();
        if Path::new(root).exists() {
            return Ok(());
        }
        if let Err(e) = std::fs::create_dir_all(root) {
            log::error!("can't create data root {} error {}", root, e);
            return Err(e.to_string());
        }
        if let Some(parent) = Path::new(root).parent().and_then(|p| p.to_str()) {
            Self::fsync_dir(parent);
        }
        Self::fsync_dir(root);
        Ok(())
    }

    fn ensure_dir(path: &str, parent: &str) -> Result<(), String> {
        match std::fs::create_dir(path) {
            Ok(_) => {
                Self::fsync_dir(parent);
                Self::fsync_dir(path);
                Ok(())
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
            Err(e) => {
                log::error!("can't create dir {} error {}", path, e);
                Err(e.to_string())
            }
        }
    }

    fn ensure_dirs(ino: Ino) -> Result<(), String> {
        Self::ensure_root_dir()?;
        let root = get_data_path().as_str();
        let dir1 = Self::build_dir1(ino);
        let dir2 = Self::build_dir(ino);
        Self::ensure_dir(&dir1, root)?;
        Self::ensure_dir(&dir2, &dir1)?;
        Ok(())
    }

    fn open_for_read(ino: Ino) -> Option<std::fs::File> {
        let fpath = Self::build_path(ino);
        match std::fs::File::options().read(true).open(&fpath) {
            Ok(fp) => Some(fp),
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    log::error!("can't open {} error {}", fpath, e);
                }
                None
            }
        }
    }

    fn open_for_write(ino: Ino) -> Option<std::fs::File> {
        if let Err(e) = Self::ensure_dirs(ino) {
            log::error!("can't prepare data dir for ino {} error {}", ino, e);
            return None;
        }
        let fpath = Self::build_path(ino);
        let dir = Self::build_dir(ino);
        match std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&fpath)
        {
            Ok(fp) => {
                Self::fsync_dir(&dir);
                Some(fp)
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                match std::fs::File::options().read(true).write(true).open(&fpath) {
                    Ok(fp) => Some(fp),
                    Err(e) => {
                        log::error!("can't open {} error {}", fpath, e);
                        None
                    }
                }
            }
            Err(e) => {
                log::error!("can't create {} error {}", fpath, e);
                None
            }
        }
    }

    fn get_fp_and_then<F, R>(key: String, ino: Ino, create: bool, f: F) -> Option<R>
    where
        F: FnOnce(&mut std::fs::File) -> R,
    {
        let mut cache = G_FILE_CACHE.lock().unwrap();
        if let Some(fp) = cache.get_mut(&key) {
            Some(f(fp))
        } else {
            let fp = if create { Self::open_for_write(ino) } else { Self::open_for_read(ino) }?;
            let fp_ref = cache.add(key, fp).unwrap();
            Some(f(fp_ref))
        }
    }

    fn sync_file(ino: Ino, datasync: bool) -> bool {
        let key = Self::file_key(ino);
        let res = Self::get_fp_and_then(key, ino, false, |fp| {
            if datasync {
                fp.sync_data().map_err(|e| e.to_string())
            } else {
                fp.sync_all().map_err(|e| e.to_string())
            }
        });
        match res {
            Some(Ok(())) => true,
            Some(Err(e)) => {
                log::error!("can't sync file {} error {}", ino, e);
                false
            }
            None => {
                log::error!("can't sync file {}", ino);
                false
            }
        }
    }

    pub fn set_len(ino: Ino, size: u64) -> Result<(), String> {
        let key = Self::file_key(ino);
        let res = Self::get_fp_and_then(key, ino, true, |fp| {
            fp.set_len(size)?;
            fp.sync_all()
        });
        match res {
            Some(Ok(())) => Ok(()),
            Some(Err(e)) => Err(e.to_string()),
            None => Err("can't open data file".to_string()),
        }
    }

    pub fn fsync(ino: Ino, datasync: bool) -> Result<(), String> {
        if Self::sync_file(ino, datasync) {
            Ok(())
        } else {
            Err("can't sync file".to_string())
        }
    }

    pub fn unlink(ino: Ino) {
        let key = Self::file_key(ino);
        if let Ok(mut cache) = G_FILE_CACHE.lock() {
            cache.del(&key);
        }
        let p = Self::build_path(ino);
        match std::fs::remove_file(&p) {
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    log::error!("can't remove {} error {}", p, e);
                }
            }
            Ok(_) => {
                let dir = Self::build_dir(ino);
                Self::fsync_dir(&dir);
                log::info!("remove file {}", p);
            }
        }
    }

    fn write_impl(&mut self, ino: Ino, e: &Entry) -> bool {
        let key = Self::file_key(ino);
        Self::get_fp_and_then(key, ino, true, |fp| unsafe {
            let s = std::slice::from_raw_parts(e.data, e.size as usize);
            let r = fp.write_at(s, e.off);
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
        let key = Self::file_key(ino);
        let sz = min(FS_FUSE_MAX_IO_SIZE, size as u64) as usize;

        Self::get_fp_and_then(key, ino, false, |fp| {
            let mut v = vec![0u8; sz];
            match fp.read_at(&mut v, off) {
                Ok(n) => {
                    v.truncate(n);
                    Some(v)
                }
                Err(e) => {
                    log::error!("can't read data ino {} off {} size {} error {}", ino, off, sz, e);
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

        for e in buf {
            sz = max(sz, e.off + e.size);
            if !self.write_impl(ino, e) {
                log::warn!("write {} fail", ino);
                return;
            }
        }

        // ensure data is on disk before updating metadata
        if !Self::sync_file(ino, true) {
            log::error!("can't sync data file for ino {}", ino);
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
