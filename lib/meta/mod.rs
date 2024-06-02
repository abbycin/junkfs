mod dentry;
mod inode;
mod meta;
mod meta_store;
mod sled;
mod super_block;

use crate::meta::meta::NameT;
use crate::store::CacheStore;
pub use inode::{Inode, Itype};
pub use meta::{Ino, Meta};
use meta_store::MetaStore;

pub trait MetaKV {
    fn key(&self) -> String;

    fn val(&self) -> Vec<u8>;
}

pub struct FileHandle {
    ino: Ino,
    pub fh: u64,
    cache: CacheStore,
}

impl FileHandle {
    pub fn new(ino: Ino, fh: u64) -> Self {
        Self {
            ino,
            fh,
            cache: CacheStore::new(ino), // TODO: we can pass config here to change store backend
        }
    }

    pub fn write(&mut self, meta: &mut Meta, off: u64, data: &[u8]) -> usize {
        self.cache.write(meta, off, data)
    }

    pub fn flush(&mut self, meta: &mut Meta) {
        self.cache.flush(meta);
    }

    pub fn read(&mut self, meta: &mut Meta, off: u64, size: usize) -> Option<Vec<u8>> {
        self.cache.read(meta, off, size)
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        log::info!("drop FileHandle ino {} fh {}", self.ino, self.fh);
    }
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

    pub fn off(&self) -> usize {
        self.pos
    }

    pub fn done(&self) -> bool {
        if self.entry.len() > 0 {
            return self.pos == self.entry.len();
        }
        return true;
    }

    pub fn next(&mut self) -> Option<&NameT> {
        if self.pos == self.entry.len() {
            None
        } else {
            let tmp = &self.entry[self.pos];
            self.pos += 1;
            Some(tmp)
        }
    }
}

impl Drop for DirHandle {
    fn drop(&mut self) {
        log::info!("drop DirHandle fh {} entry size {}", self.fh, self.entry.len());
    }
}

pub trait HandleCmp {
    fn eq(&self, fh: u64) -> bool;
}

impl HandleCmp for FileHandle {
    fn eq(&self, fh: u64) -> bool {
        self.fh == fh
    }
}

impl HandleCmp for DirHandle {
    fn eq(&self, fh: u64) -> bool {
        self.fh == fh
    }
}
