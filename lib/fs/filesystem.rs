use crate::meta::{Ino, Meta};
use crate::store::FileStore;
use crate::utils::{to_attr, to_filetype};
use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    Request, TimeOrNow,
};
use libc::{EACCES, EEXIST, EFAULT, ENOENT};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Fs {
    meta: Meta,
    store: HashMap<Ino, Vec<FileStore>>,
    file_counter: u64,
}

impl Fs {
    pub fn new(path: String) -> Result<Self, String> {
        let meta = Meta::load(path);
        if meta.is_err() {
            return Err(meta.err().unwrap());
        }

        Ok(Fs {
            meta: meta.unwrap(),
            store: HashMap::new(),
            file_counter: 0,
        })
    }

    pub fn flush_sb(&self) {
        self.meta.flush_sb();
    }

    fn new_handle(&mut self, ino: Ino) -> &mut FileStore {
        let mut r = 0;
        if self.store.len() >= self.file_counter as usize {
            self.file_counter += 1;
            r = self.file_counter;
        } else {
            for i in 1..self.file_counter {
                if !self.store.contains_key(&i) {
                    r = i;
                    break;
                }
            }
        }
        self.store.insert(ino, vec![FileStore::new(r)]);
        return self.find_handle(ino, r).unwrap();
    }

    fn find_handle(&mut self, ino: Ino, fh: u64) -> Option<&mut FileStore> {
        let v = self.store.get_mut(&ino);
        if v.is_none() {
            return None;
        }

        let v = v.unwrap();
        for i in v {
            if i.fh == fh {
                return Some(i);
            }
        }
        None
    }
}

impl Filesystem for Fs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_str().unwrap().to_string();
        if let Some(inode) = self.meta.lookup(parent, name) {
            let attr = to_attr(&inode);
            let ttl = time::Duration::new(1, 0);
            reply.entry(&ttl, &attr, 0);
        } else {
            reply.error(libc::EEXIST);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        todo!()
    }

    fn init(&mut self, req: &fuser::Request<'_>, _cfg: &mut fuser::KernelConfig) -> Result<(), i32> {
        println!(
            "unique {}, uid {}, gid {}, pid {}",
            req.unique(),
            req.uid(),
            req.gid(),
            req.pid()
        );

        Ok(())
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        todo!()
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        let r = self.meta.open(_ino, _flags);

        match r {
            None => reply.error(EEXIST),
            Some(ino) => {
                let h = self.new_handle(ino.id);
                reply.opened(h.fh, 0);
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let file = self.find_handle(ino, fh);

        match file {
            None => {
                eprintln!("can't find handle of {fh}");
                reply.error(EEXIST);
            }
            Some(file) => {
                let buf = file.read_at(ino, offset as u64, size as u64);
                match buf {
                    Err(e) => {
                        eprintln!("read fail error {}", e.to_string());
                        reply.error(EFAULT);
                    }
                    Ok(buf) => {
                        reply.data(&buf);
                    }
                }
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        todo!()
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        todo!()
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let handle = self.find_handle(ino, fh);
        if handle.is_none() {
            reply.error(EACCES);
            return;
        }
        let de = self.meta.load_dentry(ino).expect("can't load dentry");
        let mut off = 0;
        for i in &de {
            // the `offset` is used for cache
            if off > offset {
                reply.add(i.inode.id, off, to_filetype(i.inode.kind), &i.name);
            }
            off += 1;
        }
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let name = name.to_str().unwrap().to_string();
        let r = self.meta.mknod(parent, name, mode);

        match r {
            Err(e) => {
                reply.error(e);
            }
            Ok(inode) => {
                let attr = to_attr(&inode);
                let ttl = time::Duration::new(1, 0);
                reply.entry(&ttl, &attr, 0);
            }
        }
    }

    // `create` is a fuse operation of `mknod` and `open`, which is used to create regular file
    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name = name.to_str().unwrap().to_string();
        let r = self.meta.mknod(parent, name, mode);
        if r.is_err() {
            reply.error(r.err().unwrap());
            return;
        }

        let inode = r.unwrap();
        // open file
        let r = self.meta.open(inode.id, flags);

        match r {
            None => reply.error(EFAULT),
            Some(ino) => {
                let ttl = time::Duration::new(1, 0);
                let attr = to_attr(&inode);
                let handle = self.new_handle(ino.id);
                reply.created(&ttl, &attr, 0, handle.fh, 0);
            }
        }
    }
}

impl Drop for Fs {
    fn drop(&mut self) {
        self.meta.close()
    }
}
