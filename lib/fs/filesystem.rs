use crate::cache::MemPool;
use crate::meta::{DirHandle, FileHandle, Ino, Itype, Meta};
use crate::store::{CacheStore, FileStore};
#[cfg(feature = "stats")]
use crate::store::snapshot as stats_snapshot;
use crate::utils::{to_attr, to_filetype, BitMap};
use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    Request, TimeOrNow,
};
use libc::{EFAULT, EIO, ENOENT};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(feature = "stats")]
use std::time::Instant;

const WRITEBACK_INTERVAL_MS: u64 = 100;

struct Writeback {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Writeback {
    fn start(meta: Arc<Meta>, caches: Arc<Mutex<HashMap<Ino, Arc<Mutex<CacheStore>>>>>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let handle = thread::spawn(move || {
            #[cfg(feature = "stats")]
            let mut last_log = Instant::now();
            loop {
                if stop_thread.load(Ordering::Relaxed) {
                    break;
                }
            let caches_snapshot = {
                let map = caches.lock().unwrap();
                map.values().cloned().collect::<Vec<_>>()
            };
            for cache in caches_snapshot {
                let (ino, bufs) = {
                    let mut c = cache.lock().unwrap();
                    if !c.should_flush() {
                        continue;
                    }
                    let ino = c.ino();
                    let bufs = c.take_entries();
                    (ino, bufs)
                };
                if let Err(e) = CacheStore::flush_entries(ino, bufs, false) {
                    log::error!("writeback flush ino {} error {}", ino, e);
                }
            }
                let _ = meta.flush_dirty_inodes();
                #[cfg(feature = "stats")]
                {
                    if last_log.elapsed() >= Duration::from_secs(5) {
                        let s = stats_snapshot();
                        log::info!(
                            "write stats write_calls {} write_bytes {} dirty_bytes {} flush_calls {} flush_bytes {} flush_ns {} flush_err {} pwritev_calls {} pwritev_bytes {} pwritev_ns {}",
                            s.write_calls,
                            s.write_bytes,
                            s.dirty_bytes,
                            s.flush_calls,
                            s.flush_bytes,
                            s.flush_ns,
                            s.flush_errors,
                            s.pwritev_calls,
                            s.pwritev_bytes,
                            s.pwritev_ns
                        );
                        last_log = Instant::now();
                    }
                }
                thread::sleep(Duration::from_millis(WRITEBACK_INTERVAL_MS));
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

pub struct Fs {
    meta: Arc<Meta>,
    file_handles: Mutex<HashMap<u64, Arc<Mutex<FileHandle>>>>,
    dir_handles: Mutex<HashMap<u64, Arc<Mutex<DirHandle>>>>,
    inode_caches: Arc<Mutex<HashMap<Ino, Arc<Mutex<CacheStore>>>>>,
    hmap: Mutex<BitMap>,
    writeback: Writeback,
}

impl Fs {
    pub fn new(path: String) -> Result<Self, String> {
        let meta = Meta::load_fs(path);
        if meta.is_err() {
            return Err(meta.err().unwrap());
        }

        MemPool::init(100 << 20);

        let meta = Arc::new(meta.unwrap());
        let inode_caches = Arc::new(Mutex::new(HashMap::new()));
        let writeback = Writeback::start(meta.clone(), inode_caches.clone());

        Ok(Fs {
            meta,
            file_handles: Mutex::new(HashMap::new()),
            dir_handles: Mutex::new(HashMap::new()),
            inode_caches,
            hmap: Mutex::new(BitMap::new(10240)), // More handles for complex builds
            writeback,
        })
    }

    fn flush_all_caches(&self) -> bool {
        let caches = {
            let map = self.inode_caches.lock().unwrap();
            map.values().cloned().collect::<Vec<_>>()
        };
        let mut ok = true;
        for cache in caches {
            let (ino, bufs) = {
                let mut c = cache.lock().unwrap();
                let ino = c.ino();
                let bufs = c.take_entries();
                (ino, bufs)
            };
            if let Err(e) = CacheStore::flush_entries(ino, bufs, false) {
                log::error!("flush cache ino {} error {}", ino, e);
                ok = false;
            }
        }
        ok
    }

    fn new_file_handle(&self, ino: Ino) -> Option<Arc<Mutex<FileHandle>>> {
        let mut hmap = self.hmap.lock().unwrap();
        let fh = hmap.alloc()?;
        let cache = {
            let mut caches = self.inode_caches.lock().unwrap();
            caches
                .entry(ino)
                .or_insert_with(|| Arc::new(Mutex::new(CacheStore::new(ino))))
                .clone()
        };
        let entry = Arc::new(Mutex::new(FileHandle::new(ino, fh, cache)));
        self.file_handles.lock().unwrap().insert(fh, entry.clone());
        Some(entry)
    }

    fn find_file_handle(&self, fh: u64) -> Option<Arc<Mutex<FileHandle>>> {
        self.file_handles.lock().unwrap().get(&fh).cloned()
    }

    fn remove_file_handle(&self, fh: u64) {
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
        let meta = &self.meta;

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
        let meta = &self.meta;
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
        let meta = &self.meta;
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
        let meta = &self.meta;
        match meta.load_inode(ino) {
            None => reply.error(ENOENT),
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
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if let Some(x) = _size {
                    if x < inode.length {
                        let f_handles = self.file_handles.lock().unwrap();
                        for h in f_handles.values() {
                            let mut f = h.lock().unwrap();
                            if f.ino == ino {
                                f.flush(false);
                                f.clear();
                            }
                        }
                    }
                    if let Err(e) = FileStore::set_len(ino, x) {
                        log::error!("can't set_len ino {} size {} error {}", ino, x, e);
                        reply.error(EFAULT);
                        return;
                    }
                    if x != inode.length {
                        inode.mtime = now;
                        inode.ctime = now;
                    }
                    inode.length = x;
                }
                if let Some(x) = _atime {
                    inode.atime = match x {
                        TimeOrNow::Now => now,
                        TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    };
                }
                if let Some(x) = _mtime {
                    inode.mtime = match x {
                        TimeOrNow::Now => now,
                        TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    };
                }
                if let Some(x) = _ctime {
                    inode.ctime = x.duration_since(UNIX_EPOCH).unwrap().as_secs();
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

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let meta = &self.meta;
        if let Some(h) = self.find_file_handle(fh) {
            let mut f = h.lock().unwrap();
            let inode = match meta.load_inode(f.ino) {
                Some(i) => i,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            };
            if offset as u64 >= inode.length {
                reply.data(&[]);
                return;
            }
            let size = std::cmp::min(size as u64, inode.length - offset as u64) as usize;
            if let Some(data) = f.read(offset as u64, size) {
                reply.data(&data);
            } else {
                reply.error(EFAULT);
            }
        } else {
            reply.error(ENOENT);
        }
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
        let meta = &self.meta;
        if let Some(h) = self.find_file_handle(fh) {
            let mut total = 0usize;
            let mut retries = 0u32;
            while total < data.len() {
                let n = {
                    let mut f = h.lock().unwrap();
                    f.write(offset as u64 + total as u64, &data[total..])
                };
                if n == 0 {
                    retries += 1;
                    if retries > 5 {
                        reply.error(EIO);
                        return;
                    }
                    if !self.flush_all_caches() {
                        reply.error(EIO);
                        return;
                    }
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                total += n;
                retries = 0;
            }
            if total > 0 {
                let _ = meta.update_inode_after_write(ino, offset as u64 + total as u64);
            }
            reply.written(total as u32);
        } else {
            reply.error(ENOENT);
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let meta = &self.meta;
        if let Some(h) = self.find_file_handle(fh) {
            h.lock().unwrap().flush(false);
            if let Err(e) = FileStore::fsync(ino, datasync) {
                log::error!("can't fsync ino {} error {}", ino, e);
                reply.error(EFAULT);
                return;
            }
            if datasync {
                if let Err(e) = meta.flush_inode(ino) {
                    log::error!("can't sync inode {} error {}", ino, e);
                    reply.error(EFAULT);
                    return;
                }
            } else if let Err(e) = meta.sync() {
                log::error!("can't sync metadata for ino {} error {}", ino, e);
                reply.error(EFAULT);
                return;
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn flush(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        if let Some(h) = self.find_file_handle(fh) {
            let _ = h;
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.remove_file_handle(fh);
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let fh = self.hmap.lock().unwrap().alloc();
        if let Some(fh) = fh {
            let h = self.new_dir_handle(fh);
            self.meta.load_dentry(ino, &mut h.lock().unwrap());
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

    fn fsyncdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        if let Err(e) = self.meta.sync() {
            log::error!("can't fsyncdir ino {} error {}", ino, e);
            reply.error(EFAULT);
            return;
        }
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name_str = name.to_string_lossy();
        log::info!(
            "create parent {} name {} flags {} mask {}",
            parent,
            name_str,
            _flags,
            _umask
        );

        match self.meta.mknod(parent, &name_str, Itype::File, mode) {
            Err(e) => {
                log::warn!("create fail, errno {}", e);
                reply.error(e);
            }
            Ok(inode) => {
                let ino = inode.id;
                let attr = to_attr(&inode);
                if let Some(handle) = self.new_file_handle(ino) {
                    let fh = handle.lock().unwrap().fh;
                    reply.created(&time::Duration::new(1, 0), &attr, 0, fh, 0);
                } else {
                    reply.error(EFAULT);
                }
            }
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
        match self.meta.mknod(parent, name.to_string_lossy(), Itype::File, mode) {
            Ok(inode) => reply.entry(&time::Duration::new(1, 0), &to_attr(&inode), 0),
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
        match self.meta.mknod(parent, name.to_string_lossy(), Itype::Dir, mode) {
            Ok(inode) => reply.entry(&time::Duration::new(1, 0), &to_attr(&inode), 0),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.meta.unlink(parent, &name.to_string_lossy()) {
            Ok(inode) => {
                if inode.kind == Itype::File && inode.links == 0 {
                    FileStore::unlink(inode.id);
                }
                reply.ok();
            }
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.meta.unlink(parent, &name.to_string_lossy()) {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn symlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, link: &Path, reply: ReplyEntry) {
        let target = link.to_string_lossy().to_string();
        match self.meta.mknod(parent, name.to_string_lossy(), Itype::Symlink, 0o777) {
            Ok(inode) => {
                let ino = inode.id;
                let attr = to_attr(&inode);
                if let Some(h) = self.new_file_handle(ino) {
                    let mut f = h.lock().unwrap();
                    let n = f.write(0, target.as_bytes());
                    if n > 0 {
                        let _ = self.meta.update_inode_after_write(ino, n as u64);
                    }
                    f.flush(false);
                }
                reply.entry(&time::Duration::new(1, 0), &attr, 0);
            }
            Err(e) => reply.error(e),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        if let Some(inode) = self.meta.load_inode(ino) {
            let len = inode.length as usize;
            if let Some(h) = self.new_file_handle(ino) {
                let mut f = h.lock().unwrap();
                if let Some(data) = f.read(0, len) {
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
        match self.meta.link(ino, newparent, &newname.to_string_lossy()) {
            Ok(inode) => reply.entry(&time::Duration::new(1, 0), &to_attr(&inode), 0),
            Err(e) => reply.error(e),
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        match self
            .meta
            .rename(parent, &name.to_string_lossy(), newparent, &newname.to_string_lossy())
        {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }
}

impl Drop for Fs {
    fn drop(&mut self) {
        self.writeback.stop();
        let caches = {
            let map = self.inode_caches.lock().unwrap();
            map.values().cloned().collect::<Vec<_>>()
        };
        for cache in caches {
            let (ino, bufs) = {
                let mut c = cache.lock().unwrap();
                let ino = c.ino();
                let bufs = c.take_entries();
                (ino, bufs)
            };
        let _ = CacheStore::flush_entries(ino, bufs, true);
        }
        let _ = self.meta.sync();
        self.meta.close();
        #[cfg(feature = "stats")]
        {
            let s = stats_snapshot();
            log::info!(
                "final write stats write_calls {} write_bytes {} dirty_bytes {} flush_calls {} flush_bytes {} flush_ns {} flush_err {} pwritev_calls {} pwritev_bytes {} pwritev_ns {}",
                s.write_calls,
                s.write_bytes,
                s.dirty_bytes,
                s.flush_calls,
                s.flush_bytes,
                s.flush_ns,
                s.flush_errors,
                s.pwritev_calls,
                s.pwritev_bytes,
                s.pwritev_ns
            );
        }
        MemPool::destroy();
    }
}
