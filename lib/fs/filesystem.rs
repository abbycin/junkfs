use crate::cache::MemPool;
use crate::meta::{DirHandle, FileHandle, Ino, Itype, Meta};
#[cfg(feature = "stats")]
use crate::store::snapshot as stats_snapshot;
use crate::store::{CacheStore, FileStore};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const WRITEBACK_INTERVAL_MS: u64 = 100;
const META_COMMIT_INTERVAL_MS: u64 = 200;
const META_COMMIT_THRESHOLD: usize = 8192;
const HANDLE_SHARDS: usize = 64;
const INODE_SHARDS: usize = 64;

type CacheMap = HashMap<Ino, Arc<Mutex<CacheStore>>>;
type CacheShards = Arc<Vec<Mutex<CacheMap>>>;

fn handle_shard(key: u64) -> usize {
    key as usize & (HANDLE_SHARDS - 1)
}

fn inode_shard(key: Ino) -> usize {
    key as usize & (INODE_SHARDS - 1)
}

struct Writeback {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Writeback {
    fn start(meta: Arc<Meta>, caches: CacheShards) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let handle = thread::spawn(move || {
            let mut last_commit = Instant::now();
            #[cfg(feature = "stats")]
            let mut last_log = Instant::now();
            loop {
                if stop_thread.load(Ordering::Relaxed) {
                    break;
                }
                let caches_snapshot = {
                    let mut out = Vec::new();
                    for shard in caches.iter() {
                        let map = shard.lock().unwrap();
                        out.extend(map.values().cloned());
                    }
                    out
                };
                for cache in caches_snapshot {
                    let (ino, res) = {
                        let mut c = cache.lock().unwrap();
                        if !c.should_flush() {
                            continue;
                        }
                        let ino = c.ino();
                        let res = c.flush(false);
                        (ino, res)
                    };
                    if let Err(e) = res {
                        log::error!("writeback flush ino {} error {}", ino, e);
                    }
                }
                let _ = meta.flush_dirty_inodes();
                let pending = meta.pending_len();
                if pending >= META_COMMIT_THRESHOLD
                    || last_commit.elapsed() >= Duration::from_millis(META_COMMIT_INTERVAL_MS)
                {
                    if let Err(e) = meta.commit_pending() {
                        log::error!("commit pending error {}", e);
                    }
                    last_commit = Instant::now();
                }
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
    file_handles: Vec<Mutex<HashMap<u64, Arc<Mutex<FileHandle>>>>>,
    dir_handles: Vec<Mutex<HashMap<u64, Arc<Mutex<DirHandle>>>>>,
    inode_caches: CacheShards,
    inode_refs: Vec<Mutex<HashMap<Ino, usize>>>,
    orphan_inodes: Arc<Mutex<HashSet<Ino>>>,
    next_fh: AtomicU64,
    writeback: Mutex<Writeback>,
    shutdown: AtomicBool,
}

impl Fs {
    pub fn new(path: String) -> Result<Self, String> {
        let meta = Meta::load_fs(path);
        if meta.is_err() {
            return Err(meta.err().unwrap());
        }

        MemPool::init(256 << 20);

        let meta = Arc::new(meta.unwrap());
        let inode_caches: CacheShards = Arc::new((0..INODE_SHARDS).map(|_| Mutex::new(HashMap::new())).collect());
        let inode_refs = (0..INODE_SHARDS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect::<Vec<_>>();
        let file_handles = (0..HANDLE_SHARDS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect::<Vec<_>>();
        let dir_handles = (0..HANDLE_SHARDS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect::<Vec<_>>();
        let orphan_inodes = Arc::new(Mutex::new(HashSet::new()));
        let writeback = Writeback::start(meta.clone(), inode_caches.clone());

        Ok(Fs {
            meta,
            file_handles,
            dir_handles,
            inode_caches,
            inode_refs,
            orphan_inodes,
            next_fh: AtomicU64::new(1),
            writeback: Mutex::new(writeback),
            shutdown: AtomicBool::new(false),
        })
    }

    pub(crate) fn meta(&self) -> &Meta {
        self.meta.as_ref()
    }

    pub(crate) fn flush_all_caches(&self) -> bool {
        let mut caches = Vec::new();
        for shard in self.inode_caches.iter() {
            let map = shard.lock().unwrap();
            caches.extend(map.values().cloned());
        }
        let mut ok = true;
        for cache in caches {
            let (ino, res) = {
                let mut c = cache.lock().unwrap();
                let ino = c.ino();
                let res = c.flush(false);
                (ino, res)
            };
            if let Err(e) = res {
                log::error!("flush cache ino {} error {}", ino, e);
                ok = false;
            }
        }
        ok
    }

    fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn new_file_handle(&self, ino: Ino) -> Option<Arc<Mutex<FileHandle>>> {
        let fh = self.alloc_fh();
        let cache = {
            let idx = inode_shard(ino);
            let mut caches = self.inode_caches[idx].lock().unwrap();
            caches
                .entry(ino)
                .or_insert_with(|| Arc::new(Mutex::new(CacheStore::new(ino))))
                .clone()
        };
        {
            let idx = inode_shard(ino);
            let mut refs = self.inode_refs[idx].lock().unwrap();
            let entry = refs.entry(ino).or_insert(0);
            *entry += 1;
        }
        let entry = Arc::new(Mutex::new(FileHandle::new(ino, fh, cache)));
        let idx = handle_shard(fh);
        self.file_handles[idx].lock().unwrap().insert(fh, entry.clone());
        Some(entry)
    }

    pub(crate) fn find_file_handle(&self, fh: u64) -> Option<Arc<Mutex<FileHandle>>> {
        let idx = handle_shard(fh);
        self.file_handles[idx].lock().unwrap().get(&fh).cloned()
    }

    pub(crate) fn remove_file_handle(&self, fh: u64) {
        let removed = {
            let idx = handle_shard(fh);
            let mut map = self.file_handles[idx].lock().unwrap();
            map.remove(&fh)
        };
        if let Some(h) = removed {
            let ino = h.lock().unwrap().ino;
            self.drop_inode_ref(ino);
        }
    }

    fn inode_ref_count(&self, ino: Ino) -> usize {
        let idx = inode_shard(ino);
        let refs = self.inode_refs[idx].lock().unwrap();
        refs.get(&ino).copied().unwrap_or(0)
    }

    fn drop_inode_ref(&self, ino: Ino) {
        let should_drop = {
            let idx = inode_shard(ino);
            let mut refs = self.inode_refs[idx].lock().unwrap();
            match refs.get_mut(&ino) {
                Some(v) if *v > 1 => {
                    *v -= 1;
                    false
                }
                Some(_) => {
                    refs.remove(&ino);
                    true
                }
                None => false,
            }
        };
        if !should_drop {
            return;
        }
        let cache = {
            let idx = inode_shard(ino);
            let caches = self.inode_caches[idx].lock().unwrap();
            caches.get(&ino).cloned()
        };
        if let Some(cache) = cache {
            let (ino, res) = {
                let mut c = cache.lock().unwrap();
                let ino = c.ino();
                let res = c.flush(false);
                (ino, res)
            };
            if let Err(e) = res {
                log::error!("flush cache ino {} error {}", ino, e);
                return;
            }
        }
        {
            let idx = inode_shard(ino);
            let refs = self.inode_refs[idx].lock().unwrap();
            if refs.get(&ino).copied().unwrap_or(0) > 0 {
                return;
            }
        }
        {
            let idx = inode_shard(ino);
            let mut caches = self.inode_caches[idx].lock().unwrap();
            caches.remove(&ino);
        }
        let finalize = {
            let mut orphans = self.orphan_inodes.lock().unwrap();
            orphans.remove(&ino)
        };
        if finalize {
            if let Err(e) = self.meta.finalize_unlink(ino) {
                log::error!("finalize unlink ino {} error {}", ino, e);
                self.orphan_inodes.lock().unwrap().insert(ino);
                return;
            }
            FileStore::unlink(ino);
        }
    }

    pub(crate) fn unlink(&self, parent: Ino, name: &str) -> Result<crate::meta::Inode, libc::c_int> {
        let inode = self.meta.lookup(parent, name).ok_or(libc::ENOENT)?;
        if inode.kind == Itype::File && self.inode_ref_count(inode.id) > 0 {
            let inode = self.meta.unlink_keep_inode(parent, name)?;
            if inode.links == 0 {
                self.orphan_inodes.lock().unwrap().insert(inode.id);
            }
            return Ok(inode);
        }
        let inode = self.meta.unlink(parent, name)?;
        if inode.kind == Itype::File && inode.links == 0 {
            FileStore::unlink(inode.id);
        }
        Ok(inode)
    }

    pub(crate) fn rename(
        &self,
        old_parent: Ino,
        old_name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), libc::c_int> {
        self.meta
            .rename_with_unlink(old_parent, old_name, new_parent, new_name, |parent, name, target| {
                if target.kind == Itype::File && self.inode_ref_count(target.id) > 0 {
                    let inode = self.meta.unlink_keep_inode(parent, name)?;
                    if inode.links == 0 {
                        self.orphan_inodes.lock().unwrap().insert(inode.id);
                    }
                    return Ok(());
                }
                let inode = self.meta.unlink(parent, name)?;
                if inode.kind == Itype::File && inode.links == 0 {
                    FileStore::unlink(inode.id);
                }
                Ok(())
            })
    }

    pub(crate) fn new_dir_handle(&self, fh: u64) -> Arc<Mutex<DirHandle>> {
        let h = Arc::new(Mutex::new(DirHandle::new(fh)));
        let idx = handle_shard(fh);
        self.dir_handles[idx].lock().unwrap().insert(fh, h.clone());
        h
    }

    pub(crate) fn new_dir_handle_alloc(&self) -> Option<Arc<Mutex<DirHandle>>> {
        Some(self.new_dir_handle(self.alloc_fh()))
    }

    pub(crate) fn find_dir_handle(&self, fh: u64) -> Option<Arc<Mutex<DirHandle>>> {
        let idx = handle_shard(fh);
        self.dir_handles[idx].lock().unwrap().get(&fh).cloned()
    }

    pub(crate) fn remove_dir_handle(&self, fh: u64) {
        let removed = {
            let idx = handle_shard(fh);
            let mut map = self.dir_handles[idx].lock().unwrap();
            map.remove(&fh).is_some()
        };
        if removed {}
    }

    pub(crate) fn flush_open_file_handles(&self, ino: Ino) {
        let mut handles = Vec::new();
        for shard in &self.file_handles {
            let map = shard.lock().unwrap();
            handles.extend(map.values().cloned());
        }
        for h in handles {
            let mut f = h.lock().unwrap();
            if f.ino == ino {
                f.flush(false);
                f.clear();
            }
        }
    }

    pub fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::Relaxed) {
            return;
        }
        let pending_before = self.meta.pending_len();
        if pending_before > 0 {
            log::info!("shutdown pending before {}", pending_before);
        }
        if let Ok(mut w) = self.writeback.lock() {
            w.stop();
        }
        let mut caches = Vec::new();
        for shard in self.inode_caches.iter() {
            let map = shard.lock().unwrap();
            caches.extend(map.values().cloned());
        }
        for cache in caches {
            let (ino, res) = {
                let mut c = cache.lock().unwrap();
                let ino = c.ino();
                let res = c.flush(true);
                (ino, res)
            };
            if let Err(e) = res {
                log::error!("flush cache ino {} error {}", ino, e);
            }
        }
        let orphans = {
            let mut set = self.orphan_inodes.lock().unwrap();
            set.drain().collect::<Vec<_>>()
        };
        for ino in orphans {
            if let Err(e) = self.meta.finalize_unlink(ino) {
                log::error!("finalize unlink ino {} error {}", ino, e);
                self.orphan_inodes.lock().unwrap().insert(ino);
                continue;
            }
            FileStore::unlink(ino);
        }
        if let Err(e) = self.meta.sync() {
            log::error!("sync metadata on drop error {}", e);
        }
        let pending_after = self.meta.pending_len();
        if pending_after > 0 {
            log::error!("shutdown pending after {}", pending_after);
        }
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

impl Drop for Fs {
    fn drop(&mut self) {
        self.shutdown();
    }
}
