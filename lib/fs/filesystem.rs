use crate::cache::MemPool;
use crate::meta::{DirHandle, FileHandle, HandleCmp, Ino, Itype, Meta};
use crate::store::FileStore;
use crate::utils::{to_attr, to_filetype, BitMap, FS_BLK_SIZE, FS_FUSE_MAX_IO_SIZE};
use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    Request, TimeOrNow,
};
use libc::{E2BIG, EEXIST, EFAULT, ENOENT, ENOSYS, ENOTDIR, S_IFMT, S_IFREG};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::rc::Rc;
use std::time;
use std::time::SystemTime;

type HashTable<T> = RefCell<HashMap<Ino, Vec<Rc<RefCell<T>>>>>;

pub struct Fs {
    meta: Meta,
    store: HashTable<FileHandle>,
    dirs: HashTable<DirHandle>,
    hmap: BitMap,
}

unsafe impl Send for Fs {}

impl Fs {
    pub fn new(path: String) -> Result<Self, String> {
        let meta = Meta::load_fs(path);
        if meta.is_err() {
            return Err(meta.err().unwrap());
        }

        MemPool::init(100 << 20);

        Ok(Fs {
            meta: meta.unwrap(),
            dirs: RefCell::new(HashMap::new()),
            store: RefCell::new(HashMap::new()),
            hmap: BitMap::new(1024), // at most 1024 files open at same time
        })
    }

    pub fn flush_sb(&self) {
        self.meta.flush_sb().expect("can't flush sb");
    }

    fn new_file_handle(&mut self, ino: Ino) -> Option<Rc<RefCell<FileHandle>>> {
        if self.hmap.full() {
            log::warn!("too many open files");
            return None;
        }
        let r = self.hmap.alloc().unwrap();
        let entry = Rc::new(RefCell::new(FileHandle::new(ino, r)));
        if self.store.borrow().contains_key(&ino) {
            self.store.borrow_mut().get_mut(&ino).unwrap().push(entry.clone());
        } else {
            self.store.borrow_mut().insert(ino, vec![entry.clone()]);
        }
        Some(entry)
    }

    fn find_handle<T: HandleCmp>(ino: Ino, fh: u64, m: &HashTable<T>) -> Option<Rc<RefCell<T>>> {
        if let Some(v) = m.borrow_mut().get_mut(&ino) {
            for i in v {
                if i.borrow().eq(fh) {
                    return Some(i.clone());
                }
            }
        }
        None
    }

    fn remove_handle<T: HandleCmp>(ino: Ino, fh: u64, m: &HashTable<T>) -> Option<Rc<RefCell<T>>> {
        if let Some(v) = m.borrow_mut().get_mut(&ino) {
            for (index, i) in v.iter().enumerate() {
                if i.borrow().eq(fh) {
                    let r = v.remove(index);
                    return Some(r);
                }
            }
        }
        None
    }

    fn find_file_handle(&self, ino: Ino, fh: u64) -> Option<Rc<RefCell<FileHandle>>> {
        Self::find_handle(ino, fh, &self.store)
    }

    fn remove_file_handle(&mut self, ino: Ino, fh: u64) {
        let h = Self::find_handle(ino, fh, &self.store).expect("fh not found");
        h.borrow_mut().flush(&mut self.meta);
        Self::remove_handle(ino, fh, &self.store);
        let ok = self.hmap.free(fh);
        assert!(ok);
    }

    fn new_dir_handle(&mut self, ino: Ino) -> Option<Rc<RefCell<DirHandle>>> {
        if self.hmap.full() {
            log::warn!("too many open files");
            return None;
        }
        let entry = Rc::new(RefCell::new(DirHandle::new(self.hmap.alloc().unwrap())));
        if self.dirs.borrow().contains_key(&ino) {
            self.dirs.borrow_mut().get_mut(&ino).unwrap().push(entry.clone());
        } else {
            self.dirs.borrow_mut().insert(ino, vec![entry.clone()]);
        }
        Some(entry)
    }

    fn find_dir_handle(&self, ino: Ino, fh: u64) -> Option<Rc<RefCell<DirHandle>>> {
        Self::find_handle(ino, fh, &self.dirs)
    }

    fn remove_dir_handle(&mut self, ino: Ino, fh: u64) {
        Self::remove_handle(ino, fh, &self.dirs).expect("fn not found");
        let ok = self.hmap.free(fh);
        assert!(ok);
    }
}

impl Filesystem for Fs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let mut name = name.to_string_lossy().to_string();
        let ttl = time::Duration::new(1, 0);

        if name == ".." {
            if parent == 1 {
                name = ".".to_string();
            } else {
                if let Some(inode) = self.meta.load_inode(parent) {
                    assert_ne!(inode.parent, 0);
                    if let Some(inode) = self.meta.load_inode(inode.parent) {
                        if inode.kind != Itype::Dir {
                            log::warn!("lookup parent {} name {} ino {} not dir", inode.parent, name, inode.id);
                            reply.error(ENOTDIR);
                        } else {
                            let attr = &to_attr(&inode);
                            reply.entry(&ttl, attr, 0);
                        }
                        return;
                    }
                }
                log::warn!("can't load parent {} name {}", parent, name);
                reply.error(EFAULT);
                return;
            }
        }

        if let Some(inode) = self.meta.lookup(parent, &name) {
            let attr = to_attr(&inode);
            reply.entry(&ttl, &attr, 0);
        } else {
            log::info!("lookup fail parent {} name {}", parent, name);
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        log::info!("getattr ino {}", ino);
        match self.meta.load_inode(ino) {
            None => {
                log::error!("can't load inode by Ino {ino}");
                reply.error(EEXIST);
            }
            Some(inode) => {
                let attr = to_attr(&inode);
                log::info!("getattr ino {} size {}", ino, inode.length);
                let ttl = time::Duration::new(1, 0);
                reply.attr(&ttl, &attr);
            }
        }
    }

    fn init(&mut self, req: &fuser::Request<'_>, _cfg: &mut fuser::KernelConfig) -> Result<(), i32> {
        log::info!(
            "unique {}, uid {}, gid {}, pid {}",
            req.unique(),
            req.uid(),
            req.gid(),
            req.pid()
        );
        // NOTE: the root Ino is 1, in this function we must create a root if not exist
        if let Some(inode) = self.meta.load_inode(1) {
            log::info!("load root inode {} ok", inode.id);
            Ok(())
        } else {
            match self.meta.mknod(0, "/", Itype::Dir, 0o755) {
                Err(e) => {
                    log::error!("create root inode fail, error {}", e);
                    Err(e)
                }
                Ok(_) => {
                    log::info!("create root inode ok");
                    Ok(())
                }
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        log::info!("setattr ino {}", ino);
        match self.meta.load_inode(ino) {
            None => {
                log::error!("can't load inode Ino {ino}");
                reply.error(EEXIST);
            }
            Some(mut inode) => {
                if let Some(x) = mode {
                    inode.mode = x as u16;
                }
                if let Some(x) = uid {
                    inode.uid = x;
                }
                if let Some(x) = gid {
                    inode.gid = x;
                }
                // FIXME: how to handle `size` change, truncate ???
                match self.meta.store_inode(&inode) {
                    Ok(()) => {
                        let ttl = time::Duration::new(1, 0);
                        let attr = &to_attr(&inode);
                        reply.attr(&ttl, attr);
                    }
                    Err(e) => {
                        log::error!("can't store inode {} error {}", inode.id, e);
                        reply.error(EFAULT);
                    }
                }
            }
        }
    }

    /// TODO: handle `flags`
    /// - truncate
    /// - append
    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        log::info!("open ino {} flags {}", _ino, _flags);
        let r = self.new_file_handle(_ino);
        match r {
            None => {
                log::warn!("open fail, can't create handle for ino {}", _ino);
                reply.error(EFAULT)
            }
            Some(handle) => {
                log::info!("opened ino {} fh {}", _ino, handle.borrow().fh);
                reply.opened(handle.borrow().fh, 0);
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
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        log::info!("read ino {} fh {} offset {} size {}", ino, fh, offset, size);
        if size as u64 > FS_FUSE_MAX_IO_SIZE {
            log::error!("IO request too big, limit to {} bytes", FS_FUSE_MAX_IO_SIZE);
            reply.error(E2BIG);
            return;
        }
        let file = self.find_file_handle(ino, fh);

        match file {
            None => {
                log::error!("can't find handle of {fh}");
                reply.error(EEXIST);
            }
            Some(h) => {
                let mut f = h.borrow_mut();
                let buf = f.read(&mut self.meta, offset as u64, size as usize);
                match buf {
                    None => {
                        log::error!("read fail");
                        reply.error(EFAULT);
                    }
                    Some(buf) => {
                        log::info!("read ino {} fh {} nbytes {}", ino, fh, buf.len());
                        reply.data(&buf);
                    }
                }
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        log::info!("release ino {} fh {}", _ino, _fh);
        self.remove_file_handle(_ino, _fh);
        reply.ok();
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        log::info!("write ino {} fh {} offset {} size {}", ino, fh, offset, data.len());

        match self.find_file_handle(ino, fh) {
            None => {
                log::error!("can't find file by ino {} fh {}", ino, fh);
                reply.error(ENOENT);
            }
            Some(h) => {
                let mut f = h.borrow_mut();
                let nbytes = f.write(&mut self.meta, offset as u64, data);
                reply.written(nbytes as u32);
            }
        }
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        log::info!("flush ino {} fh {}", ino, fh);
        if let Some(h) = self.find_file_handle(ino, fh) {
            h.borrow_mut().flush(&mut self.meta);
            reply.ok();
        } else {
            log::error!("flush fail ino {} fh {}", ino, fh);
            reply.error(ENOENT);
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        log::info!("opendir ino {} flags {}", ino, flags);
        let r = self.new_dir_handle(ino);
        match r {
            None => {
                log::warn!("can't create new dir handle for ino {}", ino);
                reply.error(EFAULT)
            }
            Some(handle) => {
                log::info!("opened ino {} fh {}", ino, handle.borrow().fh);
                self.meta.load_dentry(ino, &handle);
                reply.opened(handle.borrow().fh, 0);
            }
        }
    }

    fn releasedir(&mut self, _req: &Request<'_>, ino: u64, fh: u64, _flags: i32, reply: ReplyEmpty) {
        log::info!("releasedir ino {} fh {}", ino, fh);
        self.remove_dir_handle(ino, fh);
        reply.ok();
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        log::info!("readdir ino {} fh {} offset {}", ino, fh, offset);
        if let Some(h) = self.find_dir_handle(ino, fh) {
            let mut off = h.borrow().off() as i64;
            while let Some(i) = h.borrow_mut().get_next() {
                if reply.add(ino, off, to_filetype(i.kind), &i.name) {
                    log::info!("add dentry buffer full, current entry {} offset {}", i.name, off);
                    break;
                }
                off += 1;
            }
            reply.ok();
        } else {
            log::warn!("this is impossible, since a directory at least has . and ..");
            reply.error(ENOENT);
        }
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let name = name.to_string_lossy().to_string();
        log::info!("mknod parent {} name {}", parent, name);

        if mode & S_IFMT != S_IFREG {
            log::warn!("non-file node is not support");
            reply.error(ENOSYS);
            return;
        }

        match self.meta.mknod(parent, name, Itype::File, mode) {
            Err(e) => {
                log::warn!("mknod fail, errno {}", e);
                reply.error(e);
            }
            Ok(inode) => {
                let attr = to_attr(&inode);
                let ttl = time::Duration::new(1, 0);
                reply.entry(&ttl, &attr, 0);
            }
        }
    }

    fn mkdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();

        log::info!("mkdir parent {} name {}", parent, name);
        match self.meta.mknod(parent, &name, Itype::Dir, mode) {
            Ok(inode) => {
                let attr = to_attr(&inode);
                let ttl = time::Duration::new(1, 0);
                reply.entry(&ttl, &attr, 0);
            }
            Err(e) => {
                log::error!("can't create dir {}, errno {}", name, e);
                reply.error(e);
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
        let name = name.to_string_lossy().to_string();
        log::info!("create parent {} name {} flags {} mask {}", parent, name, flags, umask);
        let r = self.meta.mknod(parent, &name, Itype::File, mode);
        if r.is_err() {
            let e = r.err().unwrap();
            log::warn!("create fail, errno {}", e);
            reply.error(e);
            return;
        }

        let inode = r.unwrap();
        let r = self.new_file_handle(inode.id);

        match r {
            None => {
                log::error!("create fail parent {} name {} ino {}", parent, name, inode.id);
                reply.error(EFAULT)
            }
            Some(handle) => {
                let ttl = time::Duration::new(1, 0);
                let attr = to_attr(&inode);
                let fh = handle.borrow().fh;
                log::info!(
                    "created file parent {} name {} ino {} fh {}",
                    parent,
                    name,
                    inode.id,
                    fh
                );
                reply.created(&ttl, &attr, 0, fh, 0);
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy().to_string();
        match self.meta.unlink(parent, &name) {
            Err(e) => {
                log::error!("can't find parent {} name {}", parent, name);
                reply.error(e);
            }
            Ok(inode) => {
                if inode.kind == Itype::File {
                    let mut i = 0;
                    while i <= inode.length {
                        FileStore::unlink(inode.id, i / FS_BLK_SIZE);
                        i += FS_BLK_SIZE;
                    }
                    self.store.borrow_mut().remove(&inode.id);
                }
                reply.ok();
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy().to_string();
        match self.meta.unlink(parent, &name) {
            Err(e) => {
                log::error!("rmdir fail parent {} name {} errno {}", parent, name, e);
                reply.error(e);
            }
            Ok(inode) => {
                log::info!("rmdir ok parent {} ino {} name {}", parent, inode.id, name);
                reply.ok();
            }
        }
    }
}

impl Drop for Fs {
    fn drop(&mut self) {
        self.meta.close();
        MemPool::destroy();
    }
}
