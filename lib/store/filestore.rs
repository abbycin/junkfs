use crate::cache::{Flusher, LRUCache};
use crate::meta::Ino;
use crate::store::{record_pwritev, Entry};
use crate::utils::{get_data_path, FS_FUSE_MAX_IO_SIZE};
use once_cell::sync::Lazy;
use std::cmp::min;
use std::io::ErrorKind;
use std::os::unix::prelude::AsRawFd;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;
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
        let res = Self::get_fp_and_then(key, ino, true, |fp| fp.set_len(size).map_err(|e| e.to_string()));
        match res {
            Some(Ok(())) => Ok(()),
            Some(Err(e)) => Err(e),
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

    fn write_entries_inner(fp: &mut std::fs::File, buf: &[Entry]) -> Result<(), String> {
        if buf.is_empty() {
            return Ok(());
        }
        let fd = fp.as_raw_fd();
        let max_iov = unsafe { libc::sysconf(libc::_SC_IOV_MAX) };
        let max_iov = if max_iov <= 0 { 128 } else { max_iov as usize };
        let mut i = 0;
        while i < buf.len() {
            let start_off = buf[i].off;
            let mut expected = start_off;
            let mut iovecs: Vec<libc::iovec> = Vec::new();
            while i < buf.len() && buf[i].off == expected {
                let e = &buf[i];
                iovecs.push(libc::iovec {
                    iov_base: e.data as *mut libc::c_void,
                    iov_len: e.size as usize,
                });
                expected += e.size;
                i += 1;
                if iovecs.len() >= max_iov {
                    break;
                }
            }
            let total = expected - start_off;
            let start = Instant::now();
            let n = unsafe { libc::pwritev(fd, iovecs.as_ptr(), iovecs.len() as i32, start_off as libc::off_t) };
            let ns = start.elapsed().as_nanos() as u64;
            if n < 0 {
                return Err(std::io::Error::last_os_error().to_string());
            }
            if n as u64 != total {
                return Err("short write".to_string());
            }
            record_pwritev(total, ns);
        }
        Ok(())
    }

    pub(crate) fn write_entries(ino: Ino, buf: &[Entry], sync: bool) -> Result<(), String> {
        if buf.is_empty() {
            return Ok(());
        }
        let key = Self::file_key(ino);
        let res = Self::get_fp_and_then(key, ino, true, |fp| Self::write_entries_inner(fp, buf));
        match res {
            Some(Ok(())) => {
                if sync && !Self::sync_file(ino, true) {
                    return Err("can't sync file".to_string());
                }
                Ok(())
            }
            Some(Err(e)) => Err(e),
            None => Err("can't open data file".to_string()),
        }
    }

    pub fn read_at(ino: Ino, off: u64, size: usize) -> Option<Vec<u8>> {
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
