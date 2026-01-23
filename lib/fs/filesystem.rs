use crate::cache::MemPool;
use crate::meta::{DirHandle, FileHandle, Ino, Itype, Meta};
use crate::store::FileStore;
use crate::utils::{to_attr, to_filetype, BitMap, FS_BLK_SIZE, FS_FUSE_MAX_IO_SIZE};
use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    Request, TimeOrNow,
};
use libc::{E2BIG, EEXIST, EFAULT, ENOENT, ENOSYS, ENOTDIR, S_IFMT, S_IFREG};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Fs {
    meta: Mutex<Meta>,
    file_handles: Mutex<HashMap<u64, Arc<Mutex<FileHandle>>>>,
    dir_handles: Mutex<HashMap<u64, Arc<Mutex<DirHandle>>>>,
    hmap: Mutex<BitMap>,
}

impl Fs {
    pub fn new(path: String) -> Result<Self, String> {
        let meta = Meta::load_fs(path);
        if meta.is_err() {
            return Err(meta.err().unwrap());
        }

        MemPool::init(100 << 20);

        Ok(Fs {
            meta: Mutex::new(meta.unwrap()),
            file_handles: Mutex::new(HashMap::new()),
            dir_handles: Mutex::new(HashMap::new()),
            hmap: Mutex::new(BitMap::new(10240)), // More handles for complex builds
        })
    }

    fn new_file_handle(&self, ino: Ino) -> Option<Arc<Mutex<FileHandle>>> {
        let mut hmap = self.hmap.lock().unwrap();
        let fh = hmap.alloc()?;
        let entry = Arc::new(Mutex::new(FileHandle::new(ino, fh)));
        self.file_handles.lock().unwrap().insert(fh, entry.clone());
        Some(entry)
    }

    fn find_file_handle(&self, fh: u64) -> Option<Arc<Mutex<FileHandle>>> {
        self.file_handles.lock().unwrap().get(&fh).cloned()
    }

    fn remove_file_handle(&self, fh: u64, meta: &mut Meta) {
        if let Some(h) = self.find_file_handle(fh) {
            let mut f = h.lock().unwrap();
            f.flush(meta);
        }
        self.file_handles.lock().unwrap().remove(&fh);
        self.hmap.lock().unwrap().free(fh);
    }

    fn new_dir_handle(&self, fh: u64) -> Arc<Mutex<DirHandle>> {
        let h = Arc::new(Mutex::new(DirHandle::new(fh)));
        self.dir_handles.lock().unwrap().insert(fh, h.clone());
        h
    }

    fn find_dir_handle(&self, fh: u64) -> Option<Arc<Mutex<DirHandle>>> {
        self.dir_handles.lock().unwrap().get(&fh).cloned()
    }

    fn remove_dir_handle(&self, fh: u64) {
        self.dir_handles.lock().unwrap().remove(&fh);
        self.hmap.lock().unwrap().free(fh);
    }
}

impl Filesystem for Fs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();
        let ttl = time::Duration::new(1, 0);
        let mut meta = self.meta.lock().unwrap();

        if name_str == ".." {
            if parent == 1 {
                if let Some(root) = meta.load_inode(1) {
                    reply.entry(&ttl, &to_attr(&root), 0);
                    return;
                }
            } else if let Some(inode) = meta.load_inode(parent) {
                let p_ino = if inode.parent == 0 { 1 } else { inode.parent };
                if let Some(parent_inode) = meta.load_inode(p_ino) {
                    reply.entry(&ttl, &to_attr(&parent_inode), 0);
                    return;
                }
            }
        }

        if let Some(inode) = meta.lookup(parent, &name_str) {
            let attr = to_attr(&inode);
            reply.entry(&ttl, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let meta = self.meta.lock().unwrap();
        match meta.load_inode(ino) {
            None => reply.error(ENOENT),
            Some(inode) => {
                let attr = to_attr(&inode);
                let ttl = time::Duration::new(1, 0);
                reply.attr(&ttl, &attr);
            }
        }
    }

    fn init(&mut self, _req: &Request<'_>, _cfg: &mut fuser::KernelConfig) -> Result<(), i32> {
        let meta = self.meta.lock().unwrap();
        if meta.load_inode(1).is_some() {
            Ok(())
        } else {
            log::error!("Root inode not found");
            Err(ENOENT)
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
        let mut meta = self.meta.lock().unwrap();
        match meta.load_inode(ino) {
            None => reply.error(ENOENT),
            Some(mut inode) => {
                if let Some(x) = mode { inode.mode = x as u16; }
                if let Some(x) = uid { inode.uid = x; }
                if let Some(x) = gid { inode.gid = x; }
                if let Some(x) = _size { 
                    if x < inode.length {
                        let f_handles = self.file_handles.lock().unwrap();
                        for h in f_handles.values() {
                            let mut f = h.lock().unwrap();
                            if f.ino == ino {
                                f.clear();
                            }
                        }
                    }
                    inode.length = x; 
                }
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if let Some(x) = _atime {
                    inode.atime = match x { TimeOrNow::Now => now, TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap().as_secs() };
                }
                if let Some(x) = _mtime {
                    inode.mtime = match x { TimeOrNow::Now => now, TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap().as_secs() };
                }
                meta.store_inode(&inode).unwrap();
                let attr = to_attr(&inode);
                let ttl = time::Duration::new(1, 0);
                reply.attr(&ttl, &attr);
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        if let Some(h) = self.new_file_handle(ino) {
            reply.opened(h.lock().unwrap().fh, 0);
        } else {
            reply.error(EFAULT);
        }
    }

    fn read(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        let mut meta = self.meta.lock().unwrap();
        if let Some(h) = self.find_file_handle(fh) {
            let mut f = h.lock().unwrap();
            if let Some(data) = f.read(&mut meta, offset as u64, size as usize) {
                reply.data(&data);
            } else {
                reply.error(EFAULT);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn write(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, offset: i64, data: &[u8], _write_flags: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) {
        let mut meta = self.meta.lock().unwrap();
        if let Some(h) = self.find_file_handle(fh) {
            let mut f = h.lock().unwrap();
            let n = f.write(&mut meta, offset as u64, data);
            reply.written(n as u32);
        } else {
            reply.error(ENOENT);
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
        let mut meta = self.meta.lock().unwrap();
        if let Some(h) = self.find_file_handle(fh) {
            h.lock().unwrap().flush(&mut meta);
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn flush(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let mut meta = self.meta.lock().unwrap();
        if let Some(h) = self.find_file_handle(fh) {
            h.lock().unwrap().flush(&mut meta);
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn release(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        let mut meta = self.meta.lock().unwrap();
        self.remove_file_handle(fh, &mut meta);
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let fh = self.hmap.lock().unwrap().alloc();
        if let Some(fh) = fh {
            let h = self.new_dir_handle(fh);
            let meta = self.meta.lock().unwrap();
            meta.load_dentry(ino, &mut h.lock().unwrap());
            reply.opened(fh, 0);
        } else {
            reply.error(EFAULT);
        }
    }

    fn readdir(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        if let Some(h) = self.find_dir_handle(fh) {
            let handle = h.lock().unwrap();
            let mut off = offset as usize;
            while let Some(entry) = handle.get_at(off) {
                let next_off = off + 1;
                if reply.add(entry.ino, next_off as i64, to_filetype(entry.kind), &entry.name) {
                    break;
                }
                off = next_off;
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _flags: i32, reply: ReplyEmpty) {
        self.remove_dir_handle(fh);
        reply.ok();
    }

    fn create(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, _umask: u32, _flags: i32, reply: ReplyCreate) {
        let name_str = name.to_string_lossy();
        log::info!("create parent {} name {} flags {} mask {}", parent, name_str, _flags, _umask);
        
        let mut meta = self.meta.lock().unwrap();
        match meta.mknod(parent, &name_str, Itype::File, mode) {
            Err(e) => {
                log::warn!("create fail, errno {}", e);
                reply.error(e);
            }
            Ok(inode) => {
                let ino = inode.id;
                let attr = to_attr(&inode);
                drop(meta); 
                if let Some(handle) = self.new_file_handle(ino) {
                    let fh = handle.lock().unwrap().fh;
                    reply.created(&time::Duration::new(1, 0), &attr, 0, fh, 0);
                } else {
                    reply.error(EFAULT);
                }
            }
        }
    }

    fn mknod(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, _umask: u32, _rdev: u32, reply: ReplyEntry) {
        let mut meta = self.meta.lock().unwrap();
        match meta.mknod(parent, &name.to_string_lossy(), Itype::File, mode) {
            Ok(inode) => reply.entry(&time::Duration::new(1, 0), &to_attr(&inode), 0),
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
        let mut meta = self.meta.lock().unwrap();
        match meta.mknod(parent, &name.to_string_lossy(), Itype::Dir, mode) {
            Ok(inode) => reply.entry(&time::Duration::new(1, 0), &to_attr(&inode), 0),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let mut meta = self.meta.lock().unwrap();
        match meta.unlink(parent, &name.to_string_lossy()) {
            Ok(inode) => {
                if inode.kind == Itype::File && inode.links == 0 {
                    let mut i = 0;
                    while i <= inode.length {
                        FileStore::unlink(inode.id, i / FS_BLK_SIZE);
                        i += FS_BLK_SIZE;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let mut meta = self.meta.lock().unwrap();
        match meta.unlink(parent, &name.to_string_lossy()) {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn symlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, link: &Path, reply: ReplyEntry) {
        let mut meta = self.meta.lock().unwrap();
        let target = link.to_string_lossy().to_string();
        match meta.mknod(parent, &name.to_string_lossy(), Itype::Symlink, 0o777) {
            Ok(inode) => {
                let ino = inode.id;
                let attr = to_attr(&inode);
                drop(meta); 
                if let Some(h) = self.new_file_handle(ino) {
                    let mut f = h.lock().unwrap();
                    let mut meta = self.meta.lock().unwrap(); 
                    f.write(&mut meta, 0, target.as_bytes());
                    f.flush(&mut meta);
                }
                reply.entry(&time::Duration::new(1, 0), &attr, 0);
            }
            Err(e) => reply.error(e),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let mut meta = self.meta.lock().unwrap();
        if let Some(inode) = meta.load_inode(ino) {
            let len = inode.length as usize;
            drop(meta); 
            if let Some(h) = self.new_file_handle(ino) {
                let mut f = h.lock().unwrap();
                let mut meta = self.meta.lock().unwrap(); 
                if let Some(data) = f.read(&mut meta, 0, len) {
                    reply.data(&data);
                    return;
                }
            }
        } else {
            reply.error(ENOENT);
            return;
        }
        reply.error(EFAULT);
    }

    fn link(&mut self, _req: &Request<'_>, ino: u64, newparent: u64, newname: &OsStr, reply: ReplyEntry) {
        let mut meta = self.meta.lock().unwrap();
        match meta.link(ino, newparent, &newname.to_string_lossy()) {
            Ok(inode) => reply.entry(&time::Duration::new(1, 0), &to_attr(&inode), 0),
            Err(e) => reply.error(e),
        }
    }

    fn rename(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, _flags: u32, reply: ReplyEmpty) {
        let mut meta = self.meta.lock().unwrap();
        match meta.rename(parent, &name.to_string_lossy(), newparent, &newname.to_string_lossy()) {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }
}

impl Drop for Fs {
    fn drop(&mut self) {
        let mut meta = self.meta.lock().unwrap();
        let f_handles = self.file_handles.lock().unwrap();
        for h in f_handles.values() {
            h.lock().unwrap().flush(&mut meta);
        }
        meta.close();
        MemPool::destroy();
    }
}
