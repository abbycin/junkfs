use crate::cache::{Flusher, LRUCache};
use crate::meta::{Ino, Meta};
use crate::store::{Entry, Store};
use crate::utils::{get_data_path, FS_BLK_SIZE, FS_FUSE_MAX_IO_SIZE};
use once_cell::sync::Lazy;
use std::cmp::{max, min};
use std::io::Write;
use std::os::unix::prelude::FileExt;
const MAX_CACHE_ITEMS: usize = 256;

struct FileFlusher;

static mut G_LUSHER: FileFlusher = FileFlusher;

impl Flusher<String, std::fs::File> for FileFlusher {
    fn flush(&mut self, key: String, data: std::fs::File) {
        let mut file = data;
        file.flush().expect(&format!("can't flush file {}", key));
        drop(file);
    }
}

static mut G_FILE_CACHE: Lazy<LRUCache<String, std::fs::File>> = Lazy::new(|| {
    let mut c = LRUCache::new(MAX_CACHE_ITEMS);
    let p = unsafe { std::ptr::addr_of_mut!(G_LUSHER) };
    c.set_backend(p);
    c
});

fn cache_add<'a>(key: String, val: std::fs::File) -> Option<&'a mut std::fs::File> {
    unsafe { G_FILE_CACHE.add(key, val) }
}

fn cache_get_mut<'a>(key: &String) -> Option<&'a mut std::fs::File> {
    unsafe { G_FILE_CACHE.get_mut(key) }
}

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

    fn get_fp<'a, 'b>(key: String, ino: Ino, blk: u64) -> Option<&'b mut std::fs::File>
    where
        'a: 'b,
    {
        if let Some(tmp) = cache_get_mut(&key) {
            Some(tmp)
        } else {
            let _ = std::fs::create_dir_all(&Self::build_dir(ino));
            let fpath = Self::build_path(ino, blk);
            // NOTE: do NOT use append, see `File::write_at` doc `pwrite64` bug
            let f = std::fs::File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(&fpath);
            if f.is_err() {
                log::error!("can't create {}", fpath);
                return None;
            }
            cache_add(key, f.unwrap())
        }
    }
    fn write_impl(&mut self, ino: Ino, e: &Entry) -> bool {
        let key = Self::write_key(ino, e.blk_id);
        let fp = Self::get_fp(key, ino, e.blk_id);

        if fp.is_none() {
            log::error!("can't open file {}_{}", ino, e.blk_id);
            return false;
        }

        let fp = fp.unwrap();
        unsafe {
            let s = std::slice::from_raw_parts(e.data, e.size as usize);
            let r = fp.write_at(s, e.blk_off);
            if r.is_err() {
                log::error!("can't write entry {:?}", e);
                return false;
            }
        }
        return true;
    }

    fn read_impl(&mut self, ino: Ino, off: u64, size: usize) -> Option<Vec<u8>> {
        let blk_id = off / FS_BLK_SIZE;
        let key = Self::read_key(ino, blk_id);
        let fp = Self::get_fp(key, ino, blk_id);
        if fp.is_none() {
            log::error!("can't open file for read {}_{}", ino, blk_id);
            return None;
        }
        let fp = fp.unwrap();
        let mut sz = min(FS_FUSE_MAX_IO_SIZE, size as u64);
        // check off + sz is cross chunk, if so, read at most rest bytes in current block
        if (off + sz) / FS_BLK_SIZE == (blk_id + 1) {
            sz = (blk_id + 1) * FS_BLK_SIZE - off;
        }
        let mut v = vec![0u8; sz as usize];
        let buf = v.as_mut_slice();
        let r = fp.read_at(buf, off % FS_BLK_SIZE);
        if r.is_err() {
            log::error!(
                "can't read data blk_id {} off {} size {}",
                blk_id,
                off % FS_BLK_SIZE,
                sz
            );
            return None;
        }
        Some(v)
    }
}

impl Store for FileStore {
    fn write(&mut self, meta: &mut Meta, ino: Ino, buf: &Vec<Entry>) {
        if buf.is_empty() {
            return;
        }
        let mut sz = 0;
        let mut inode = meta.load_inode(ino).unwrap();

        for e in buf {
            sz = max(sz, e.off + e.size);
            log::info!(
                "write off {} size {} inode.length {} size {}",
                e.off,
                e.size,
                inode.length,
                sz
            );
            if !self.write_impl(ino, e) {
                log::warn!("write {}_{} fail", ino, e.blk_id);
                return;
            }
        }

        // try update inode.length
        if inode.length < sz {
            log::info!("trying to update inode.length {} to {}", inode.length, sz);
            inode.length = sz;
            meta.store_inode(&inode).unwrap()
        }
    }

    fn read(&mut self, ino: Ino, off: u64, size: usize) -> Option<Vec<u8>> {
        self.read_impl(ino, off, size)
    }
}
