mod dentry;
mod imap;
mod inode;
mod kvstore;
mod meta_data;
mod super_block;

use crate::meta::meta_data::NameT;
use crate::store::CacheStore;
pub use inode::{Inode, Itype};
pub use meta_data::{Ino, Meta};
use std::sync::{Arc, Mutex};

pub trait MetaKV {
    fn key(&self) -> String;

    fn val(&self) -> Vec<u8>;
}

pub struct FileHandle {
    pub ino: Ino,
    pub fh: u64,
    cache: Arc<Mutex<CacheStore>>,
}

impl FileHandle {
    pub fn new(ino: Ino, fh: u64, cache: Arc<Mutex<CacheStore>>) -> Self {
        Self { ino, fh, cache }
    }

    pub fn write(&mut self, off: u64, data: &[u8]) -> usize {
        let mut cache = self.cache.lock().unwrap();
        if let Some(n) = cache.write_maybe_direct(off, data) {
            return n;
        }
        cache.write(off, data)
    }

    pub fn flush(&mut self, sync: bool) {
        let _ = self.cache.lock().unwrap().flush(sync);
    }

    pub fn clear(&mut self) {
        self.cache.lock().unwrap().clear();
    }

    pub fn read(&mut self, off: u64, size: usize) -> Option<Vec<u8>> {
        self.cache.lock().unwrap().read(off, size)
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {}
}

pub struct DirHandle {
    pub fh: u64,
    pos: usize,
    entry: Vec<NameT>,
}

impl DirHandle {
    pub fn new(fh: u64) -> Self {
        Self {
            fh,
            pos: 0,
            entry: Vec::new(),
        }
    }

    pub fn add(&mut self, e: NameT) {
        self.entry.push(e);
    }

    pub fn seek(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub fn off(&self) -> usize {
        self.pos
    }

    pub fn done(&self) -> bool {
        if !self.entry.is_empty() {
            return self.pos == self.entry.len();
        }
        true
    }

    pub fn get_next(&mut self) -> Option<&NameT> {
        if self.pos == self.entry.len() {
            None
        } else {
            let tmp = &self.entry[self.pos];
            self.pos += 1;
            Some(tmp)
        }
    }

    pub fn get_at(&self, pos: usize) -> Option<&NameT> {
        self.entry.get(pos)
    }
}

impl Drop for DirHandle {
    fn drop(&mut self) {
        log::info!("drop DirHandle fh {} entry size {}", self.fh, self.entry.len());
    }
}
