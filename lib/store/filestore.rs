use crate::meta::{Ino, Meta};
use crate::store::{Entry, Store};
use crate::utils::{get_data_path, FS_BLK_SIZE, FS_FUSE_MAX_IO_SIZE};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::os::unix::prelude::FileExt;

pub struct FileStore {
    w_ofs: HashMap<u64, std::fs::File>, // key is blk_id
    r_ofs: HashMap<u64, std::fs::File>,
}

fn build_path(ino: Ino, blk: u64) -> String {
    format!("{}/{}/{}", get_data_path(), ino, blk)
}

fn build_dir(ino: Ino) -> String {
    format!("{}/{}", get_data_path(), ino)
}

impl FileStore {
    pub fn new() -> Self {
        Self {
            w_ofs: HashMap::new(),
            r_ofs: HashMap::new(),
        }
    }

    pub fn unlink(ino: Ino, blk_id: u64) {
        let p = build_path(ino, blk_id);
        // it's not necessary to remove key from ofs, since the whole FileStore
        // will be dropped after `unlink``
        match std::fs::remove_file(&p) {
            Err(e) => {
                log::error!("can't remove {} error {}", p, e);
            }
            Ok(_) => {
                log::info!("remove file {}", p);
            }
        }
    }

    fn get_fp<'a, 'b>(m: &'a mut HashMap<u64, std::fs::File>, ino: Ino, key: u64) -> Option<&'b mut std::fs::File>
    where
        'a: 'b,
    {
        let tmp = m.contains_key(&key);
        if !tmp {
            let _ = std::fs::create_dir_all(&build_dir(ino));
            let fpath = build_path(ino, key);
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
            m.insert(key, f.unwrap());
            return m.get_mut(&key);
        } else {
            m.get_mut(&key)
        }
    }
    fn write_impl(&mut self, ino: Ino, e: &Entry) -> bool {
        let fp = Self::get_fp(&mut self.w_ofs, ino, e.blk_id);

        if fp.is_none() {
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
        let fp = Self::get_fp(&mut self.r_ofs, ino, blk_id);
        if fp.is_none() {
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
