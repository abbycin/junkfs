use crate::cache::LRUCache;
use crate::meta::dentry::Dentry;
use crate::meta::imap::InoMap;
use crate::meta::inode::{Inode, Itype};
use crate::meta::kvstore::MaceStore;
use crate::meta::super_block::SuperBlock;
use crate::meta::{DirHandle, MetaKV};
use crate::utils::{init_data_path, BitMap64};
use libc::{EEXIST, EFAULT, ENOENT, ENOTEMPTY};
use mace::{Mace, OpCode, Options};
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub type Ino = u64;

pub struct NameT {
    pub name: String,
    pub kind: Itype,
    pub ino: Ino,
}

struct MetaState {
    sb: SuperBlock,
    imap: InoMap,
}

#[derive(Copy, Clone)]
struct InodeCache {
    inode: Inode,
    dirty: bool,
}

pub struct Meta {
    pub meta: MaceStore,
    state: Mutex<MetaState>,
    inode_cache: RwLock<HashMap<Ino, InodeCache>>,
    pending: Mutex<Pending>,
    dentry_cache: Mutex<LRUCache<String, DentryCacheValue>>,
    dir_index: Mutex<HashMap<Ino, DirIndex>>,
}

struct Pending {
    puts: HashMap<String, Vec<u8>>,
    dels: HashSet<String>,
}

impl Pending {
    fn new() -> Self {
        Self {
            puts: HashMap::new(),
            dels: HashSet::new(),
        }
    }
}

enum PendingValue {
    Put(Vec<u8>),
    Deleted,
    Missing,
}

#[derive(Clone, Copy)]
enum DentryCacheValue {
    Present(Ino),
    Absent,
}

struct DirIndex {
    loaded: bool,
    entries: HashMap<String, Ino>,
}

const DENTRY_CACHE_CAP: usize = 1 << 18;

impl Meta {
    // write superblock
    pub fn format(meta_path: &str, store_path: &str) -> Result<(), String> {
        let mut opt = Options::new(meta_path);
        opt.concurrent_write = 4;
        let db = Mace::new(opt.validate().unwrap()).map_err(|e| format!("{:?}", e))?;
        let bucket = MaceStore::open_bucket(&db).map_err(|e| format!("{:?}", e))?;
        let sb = SuperBlock::new(store_path);
        let mut imap = InoMap::new(sb.total_inodes(), sb.group_size());
        // reserve ino 0
        imap.reserve(0);

        // alloc root inode id (1)
        let plan = imap
            .alloc_plan(&mut |_gid| Err("imap group not loaded".to_string()))?
            .expect("can't alloc root ino");
        let root_ino = plan.ino;
        imap.apply_alloc(plan);
        assert_eq!(root_ino, 1);

        let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let root_inode = Inode {
            id: root_ino,
            parent: 0, // root has no parent
            kind: Itype::Dir,
            mode: 0o755,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            atime: epoch,
            mtime: epoch,
            ctime: epoch,
            length: 0,
            links: 2, // . and ..
        };

        let kv = bucket.begin().expect("can't fail");
        kv.put(SuperBlock::key(), sb.val()).unwrap();
        kv.put(Inode::key(root_ino), Inode::val(&root_inode)).unwrap();
        kv.put(InoMap::summary_key(), imap.summary_val()).unwrap();
        for gid in 0..imap.group_count() {
            kv.put(InoMap::group_key(gid), imap.group_val(gid)).unwrap();
        }

        kv.commit().map_err(|e| e.to_string())
    }

    pub fn load_fs(path: String) -> Result<Self, String> {
        let meta = MaceStore::new(&path);
        let sb = meta.get(&SuperBlock::key());
        match sb {
            Err(OpCode::NotFound) => Err("not formated".to_string()),
            Err(e) => Err(e.to_string()),
            Ok(sb) => {
                let sb = bincode::deserialize::<SuperBlock>(&sb);

                match sb {
                    Err(e) => Err(e.to_string()),
                    Ok(sb) => {
                        // TODO: check consistency
                        sb.check();
                        if sb.version() != 3 {
                            return Err("unsupported superblock version".to_string());
                        }
                        let sum = meta.get(&InoMap::summary_key()).map_err(|e| e.to_string())?;
                        let summary = bincode::deserialize::<BitMap64>(&sum).map_err(|e| e.to_string())?;
                        let mut imap = InoMap::from_summary(sb.total_inodes(), sb.group_size(), summary);
                        Self::repair_imap_summary(&meta, &sb, &mut imap)?;
                        imap.check();
                        init_data_path(sb.uri());
                        let state = MetaState { sb, imap };
                        Ok(Meta {
                            meta,
                            state: Mutex::new(state),
                            inode_cache: RwLock::new(HashMap::new()),
                            pending: Mutex::new(Pending::new()),
                            dentry_cache: Mutex::new(LRUCache::new(DENTRY_CACHE_CAP)),
                            dir_index: Mutex::new(HashMap::new()),
                        })
                    }
                }
            }
        }
    }

    fn stage_put(&self, key: String, val: Vec<u8>) {
        self.maybe_cache_dentry_put(&key, &val);
        self.maybe_index_dentry_put(&key, &val);
        let mut pending = self.pending.lock().unwrap();
        pending.dels.remove(&key);
        pending.puts.insert(key, val);
    }

    fn stage_del(&self, key: String) {
        self.maybe_cache_dentry_del(&key);
        self.maybe_index_dentry_del(&key);
        let mut pending = self.pending.lock().unwrap();
        pending.puts.remove(&key);
        pending.dels.insert(key);
    }

    fn dentry_cache_get(&self, key: &str) -> Option<DentryCacheValue> {
        let mut cache = self.dentry_cache.lock().unwrap();
        let k = key.to_string();
        cache.get_mut(&k).copied()
    }

    fn dentry_cache_put(&self, key: String, val: DentryCacheValue) {
        let mut cache = self.dentry_cache.lock().unwrap();
        cache.add(key, val);
    }

    fn maybe_cache_dentry_put(&self, key: &str, val: &[u8]) {
        if !key.starts_with("d_") {
            return;
        }
        if let Ok(de) = bincode::deserialize::<Dentry>(val) {
            self.dentry_cache_put(key.to_string(), DentryCacheValue::Present(de.ino));
        }
    }

    fn maybe_cache_dentry_del(&self, key: &str) {
        if !key.starts_with("d_") {
            return;
        }
        self.dentry_cache_put(key.to_string(), DentryCacheValue::Absent);
    }

    fn maybe_index_dentry_put(&self, key: &str, val: &[u8]) {
        if let Some((parent, name)) = Self::parse_dentry_key(key) {
            if let Ok(de) = bincode::deserialize::<Dentry>(val) {
                self.dir_index_put(parent, name, de.ino);
            }
        }
    }

    fn maybe_index_dentry_del(&self, key: &str) {
        if let Some((parent, name)) = Self::parse_dentry_key(key) {
            self.dir_index_del(parent, &name);
        }
    }

    fn pending_for_prefix(&self, prefix: &str) -> (Vec<(String, Vec<u8>)>, HashSet<String>) {
        let pending = self.pending.lock().unwrap();
        let puts = pending
            .puts
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        let dels = pending
            .dels
            .iter()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect::<HashSet<_>>();
        (puts, dels)
    }

    fn parse_dentry_key(key: &str) -> Option<(Ino, String)> {
        if !key.starts_with("d_") {
            return None;
        }
        let rest = &key[2..];
        let mut it = rest.splitn(2, '_');
        let parent_str = it.next()?;
        let name = it.next()?;
        let parent = parent_str.parse::<Ino>().ok()?;
        Some((parent, name.to_string()))
    }

    fn dir_index_put(&self, parent: Ino, name: String, ino: Ino) {
        let mut map = self.dir_index.lock().unwrap();
        let idx = map.entry(parent).or_insert(DirIndex {
            loaded: false,
            entries: HashMap::new(),
        });
        idx.entries.insert(name, ino);
    }

    fn dir_index_del(&self, parent: Ino, name: &str) {
        let mut map = self.dir_index.lock().unwrap();
        if let Some(idx) = map.get_mut(&parent) {
            idx.entries.remove(name);
        }
    }

    fn dir_index_lookup(&self, parent: Ino, name: &str) -> Option<Option<Ino>> {
        let map = self.dir_index.lock().unwrap();
        if let Some(idx) = map.get(&parent) {
            if idx.loaded {
                return Some(idx.entries.get(name).copied());
            }
        }
        None
    }

    fn dir_index_has_entries(&self, ino: Ino) -> Option<bool> {
        let map = self.dir_index.lock().unwrap();
        if let Some(idx) = map.get(&ino) {
            if idx.loaded {
                return Some(!idx.entries.is_empty());
            }
        }
        None
    }

    fn build_dir_index(&self, parent: Ino) -> HashMap<String, Ino> {
        let prefix = Dentry::prefix(parent);
        let mut entries = HashMap::new();
        let view = self.meta.view();
        let iter = view.seek(&prefix);
        for item in iter {
            if !item.key().starts_with(prefix.as_bytes()) {
                break;
            }
            let de = bincode::deserialize::<Dentry>(item.val()).expect("can't deserialize dentry");
            entries.insert(de.name, de.ino);
        }
        let (pending_puts, pending_dels) = self.pending_for_prefix(&prefix);
        for key in pending_dels {
            if let Some((_, name)) = Self::parse_dentry_key(&key) {
                entries.remove(&name);
            }
        }
        for (_, data) in pending_puts {
            let de = bincode::deserialize::<Dentry>(&data).expect("can't deserialize dentry");
            entries.insert(de.name, de.ino);
        }
        entries
    }

    fn ensure_dir_index_loaded(&self, parent: Ino) {
        let need_load = {
            let map = self.dir_index.lock().unwrap();
            match map.get(&parent) {
                Some(idx) => !idx.loaded,
                None => true,
            }
        };
        if !need_load {
            return;
        }
        let entries = self.build_dir_index(parent);
        let mut map = self.dir_index.lock().unwrap();
        let idx = map.entry(parent).or_insert(DirIndex {
            loaded: true,
            entries: HashMap::new(),
        });
        idx.entries = entries;
        idx.loaded = true;
    }

    fn pending_get(&self, key: &str) -> PendingValue {
        let pending = self.pending.lock().unwrap();
        if pending.dels.contains(key) {
            return PendingValue::Deleted;
        }
        if let Some(v) = pending.puts.get(key) {
            return PendingValue::Put(v.clone());
        }
        PendingValue::Missing
    }

    pub fn commit_pending(&self) -> Result<(), String> {
        let (puts, dels) = {
            let pending = self.pending.lock().unwrap();
            if pending.puts.is_empty() && pending.dels.is_empty() {
                return Ok(());
            }
            (
                pending.puts.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>(),
                pending.dels.iter().cloned().collect::<Vec<_>>(),
            )
        };
        let kv = self.meta.begin().map_err(|e| e.to_string())?;
        for (k, v) in &puts {
            kv.upsert(k, v).map_err(|e| e.to_string())?;
        }
        for k in &dels {
            kv.del(k).map_err(|e| e.to_string())?;
        }
        kv.commit().map_err(|e| e.to_string())?;
        let mut pending = self.pending.lock().unwrap();
        for (k, v) in puts {
            if let Some(cur) = pending.puts.get(&k) {
                if *cur == v {
                    pending.puts.remove(&k);
                }
            }
        }
        for k in dels {
            if pending.dels.contains(&k) && !pending.puts.contains_key(&k) {
                pending.dels.remove(&k);
            }
        }
        Ok(())
    }

    pub fn pending_len(&self) -> usize {
        let pending = self.pending.lock().unwrap();
        pending.puts.len() + pending.dels.len()
    }

    fn dir_has_entries(&self, ino: Ino) -> bool {
        if let Some(has) = self.dir_index_has_entries(ino) {
            return has;
        }
        let prefix = Dentry::prefix(ino);
        let (has_puts, dels) = {
            let pending = self.pending.lock().unwrap();
            let has_puts = pending.puts.keys().any(|k| k.starts_with(&prefix));
            let dels = pending
                .dels
                .iter()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect::<HashSet<_>>();
            (has_puts, dels)
        };
        if has_puts {
            return true;
        }
        let view = self.meta.view();
        let mut it = view.seek(&prefix);
        while let Some(item) = it.next() {
            if !item.key().starts_with(prefix.as_bytes()) {
                break;
            }
            let key = String::from_utf8_lossy(item.key()).to_string();
            if dels.contains(&key) {
                continue;
            }
            return true;
        }
        false
    }

    fn repair_imap_summary(meta: &MaceStore, sb: &SuperBlock, imap: &mut InoMap) -> Result<(), String> {
        let mut new_summary = BitMap64::new(sb.group_count());
        for gid in 0..sb.group_count() {
            let key = InoMap::group_key(gid);
            let data = meta.get(&key).map_err(|e| e.to_string())?;
            let group = bincode::deserialize::<BitMap64>(&data).map_err(|e| e.to_string())?;
            let start = gid * sb.group_size();
            let end = std::cmp::min(sb.total_inodes(), start + sb.group_size());
            if group.cap() != end - start {
                return Err("imap group size mismatch".to_string());
            }
            if !group.full() {
                new_summary.set(gid);
            }
        }
        if &new_summary != imap.summary() {
            let val = bincode::serialize(&new_summary).map_err(|e| e.to_string())?;
            meta.insert(&InoMap::summary_key(), &val).map_err(|e| e.to_string())?;
            imap.replace_summary(new_summary);
        }
        Ok(())
    }

    fn load_imap_group(meta: &MaceStore, gid: u64) -> Result<BitMap64, String> {
        let key = InoMap::group_key(gid);
        let data = meta.get(&key).map_err(|e| e.to_string())?;
        bincode::deserialize::<BitMap64>(&data).map_err(|e| e.to_string())
    }

    fn cache_get(&self, ino: Ino) -> Option<Inode> {
        let cache = self.inode_cache.read().unwrap();
        cache.get(&ino).map(|e| e.inode)
    }

    fn cache_put(&self, inode: Inode, dirty: bool) {
        let mut cache = self.inode_cache.write().unwrap();
        cache.insert(
            inode.id,
            InodeCache {
                inode,
                dirty,
            },
        );
    }

    fn cache_mark_dirty(&self, inode: Inode) {
        let mut cache = self.inode_cache.write().unwrap();
        match cache.get_mut(&inode.id) {
            Some(e) => {
                e.inode = inode;
                e.dirty = true;
            }
            None => {
                cache.insert(
                    inode.id,
                    InodeCache {
                        inode,
                        dirty: true,
                    },
                );
            }
        }
    }

    fn cache_remove(&self, ino: Ino) {
        let mut cache = self.inode_cache.write().unwrap();
        cache.remove(&ino);
    }

    pub fn update_inode_after_write(&self, ino: Ino, end_off: u64) -> Result<(), String> {
        let mut inode = match self.cache_get(ino) {
            Some(i) => i,
            None => self.load_inode(ino).ok_or_else(|| "can't load inode".to_string())?,
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("can't get unix timestamp")
            .as_secs();
        inode.mtime = now;
        inode.ctime = now;
        if inode.length < end_off {
            inode.length = end_off;
        }
        self.cache_mark_dirty(inode);
        Ok(())
    }

    pub fn flush_dirty_inodes(&self) -> Result<(), String> {
        let dirty = {
            let cache = self.inode_cache.read().unwrap();
            cache.values().filter(|e| e.dirty).map(|e| e.inode).collect::<Vec<_>>()
        };
        if dirty.is_empty() {
            return Ok(());
        }
        for inode in &dirty {
            self.stage_put(Inode::key(inode.id), inode.val());
        }
        let mut cache = self.inode_cache.write().unwrap();
        for inode in dirty {
            if let Some(e) = cache.get_mut(&inode.id) {
                if e.inode == inode {
                    e.dirty = false;
                }
            }
        }
        Ok(())
    }

    pub fn flush_inode(&self, ino: Ino) -> Result<(), String> {
        let entry = {
            let cache = self.inode_cache.read().unwrap();
            cache.get(&ino).copied()
        };
        let Some(entry) = entry else {
            return Ok(());
        };
        if !entry.dirty {
            return Ok(());
        }
        self.stage_put(Inode::key(entry.inode.id), entry.inode.val());
        let mut cache = self.inode_cache.write().unwrap();
        if let Some(e) = cache.get_mut(&ino) {
            if e.inode == entry.inode {
                e.dirty = false;
            }
        }
        Ok(())
    }

    pub fn store(&self, key: &str, value: &[u8]) {
        self.stage_put(key.to_string(), value.to_vec());
    }

    pub fn load(&self, key: &str) -> Option<Vec<u8>> {
        match self.pending_get(key) {
            PendingValue::Put(v) => Some(v),
            PendingValue::Deleted => None,
            PendingValue::Missing => match self.meta.get_optional(key) {
                Ok(Some(x)) => Some(x),
                Ok(None) => None,
                Err(e) => {
                    log::error!("can't load key {} error {}", key, e);
                    None
                }
            },
        }
    }

    pub fn close(&self) {}

    pub fn sync(&self) -> Result<(), String> {
        self.flush_dirty_inodes()?;
        self.commit_pending()?;
        self.meta.sync().map_err(|e| e.to_string())
    }

    pub fn flush_sb(&self) -> Result<(), String> {
        let sb = { self.state.lock().unwrap().sb.clone() };
        match self.meta.insert(&SuperBlock::key(), &sb.val()) {
            Err(e) => {
                log::error!("can't flush superblock, error {}", e);
                Err(e.to_string())
            }
            Ok(_) => Ok(()),
        }
    }

    /// - use `parent` and `name` to build dentry key
    /// - load value of dentry key
    /// - if existed, load Inode from database
    /// - or else, return None
    pub fn lookup(&self, parent: Ino, name: &str) -> Option<Inode> {
        let key = Dentry::key(parent, name);
        match self.pending_get(&key) {
            PendingValue::Put(dentry) => {
                let dentry = bincode::deserialize::<Dentry>(&dentry).expect("can't deserialize dentry");
                self.load_inode(dentry.ino)
            }
            PendingValue::Deleted => None,
            PendingValue::Missing => match self.dentry_cache_get(&key) {
                Some(DentryCacheValue::Present(ino)) => self.load_inode(ino),
                Some(DentryCacheValue::Absent) => None,
                None => {
                    self.ensure_dir_index_loaded(parent);
                    match self.dir_index_lookup(parent, name) {
                        Some(Some(ino)) => {
                            self.dentry_cache_put(key, DentryCacheValue::Present(ino));
                            self.load_inode(ino)
                        }
                        Some(None) => {
                            self.dentry_cache_put(key, DentryCacheValue::Absent);
                            None
                        }
                        None => match self.meta.get_optional(&key) {
                            Ok(Some(dentry)) => {
                                let dentry = bincode::deserialize::<Dentry>(&dentry).expect("can't deserialize dentry");
                                self.dentry_cache_put(key, DentryCacheValue::Present(dentry.ino));
                                self.load_inode(dentry.ino)
                            }
                            Ok(None) => {
                                self.dentry_cache_put(key, DentryCacheValue::Absent);
                                None
                            }
                            Err(_) => None,
                        },
                    }
                }
            },
        }
    }

    pub fn mknod(&self, parent: u64, name: impl AsRef<str>, ftype: Itype, mode: u32) -> Result<Inode, libc::c_int> {
        if self.dentry_exist(parent, name.as_ref()) {
            log::error!("node existed dentry {}", Dentry::key(parent, name.as_ref()));
            return Err(EEXIST);
        }

        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("can't get unix timestamp")
            .as_secs();

        let meta = &self.meta;
        let mut state = self.state.lock().unwrap();
        let plan = state
            .imap
            .alloc_plan(&mut |gid| Self::load_imap_group(meta, gid))
            .map_err(|_| EFAULT)?
            .ok_or(ENOENT)?;
        let ino = plan.ino;

        let inode = Inode {
            id: ino,
            parent,
            kind: ftype,
            mode: mode as u16,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            atime: epoch,
            mtime: epoch,
            ctime: epoch,
            length: 0,
            links: 1,
        };

        let dkey = Dentry::key(parent, name.as_ref());
        let de = Dentry::new(parent, ino, name.as_ref());

        self.stage_put(InoMap::summary_key(), plan.summary_val());
        self.stage_put(InoMap::group_key(plan.gid), plan.group_val());
        self.stage_put(Inode::key(ino), inode.val());
        self.stage_put(dkey, de.val());

        state.imap.apply_alloc(plan);
        drop(state);
        self.cache_put(inode, false);
        Ok(inode)
    }

    pub fn unlink(&self, parent: Ino, name: &str) -> Result<Inode, libc::c_int> {
        let mut inode = self.lookup(parent, name).ok_or(ENOENT)?;

        if inode.kind == Itype::Dir {
            if self.dir_has_entries(inode.id) {
                return Err(ENOTEMPTY);
            }
        }

        let dkey = Dentry::key(parent, name);

        if inode.kind != Itype::Dir && inode.links > 1 {
            inode.links -= 1;
            inode.ctime = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("can't get unix timestamp")
                .as_secs();
            self.stage_put(Inode::key(inode.id), inode.val());
            self.stage_del(dkey);
            self.cache_put(inode, false);
            return Ok(inode);
        }

        let meta = &self.meta;
        let mut state = self.state.lock().unwrap();
        let plan = state
            .imap
            .free_plan(inode.id, &mut |gid| Self::load_imap_group(meta, gid))
            .map_err(|_| EFAULT)?
            .ok_or(EFAULT)?;
        debug_assert_eq!(plan.ino, inode.id);

        self.stage_del(Inode::key(inode.id));
        self.stage_del(dkey);
        self.stage_put(InoMap::summary_key(), plan.summary_val());
        self.stage_put(InoMap::group_key(plan.gid), plan.group_val());

        state.imap.apply_free(plan);
        drop(state);
        self.cache_remove(inode.id);
        inode.links = 0;
        Ok(inode)
    }

    pub fn load_inode(&self, inode: Ino) -> Option<Inode> {
        if let Some(i) = self.cache_get(inode) {
            return Some(i);
        }
        let key = Inode::key(inode);
        match self.meta.get(&key) {
            Err(e) => {
                log::error!("load inode error {}", e);
                None
            }
            Ok(tmp) => {
                let inode = bincode::deserialize::<Inode>(&tmp);
                if inode.is_err() {
                    log::error!("deserialize inode fail error {}", inode.err().unwrap());
                    return None;
                }
                let inode = inode.unwrap();
                self.cache_put(inode, false);
                Some(inode)
            }
        }
    }

    /// if `key` exist, we can overwrite it
    pub fn store_inode(&self, inode: &Inode) -> Result<(), String> {
        self.cache_mark_dirty(*inode);
        Ok(())
    }

    pub fn load_dentry(&self, ino: Ino, h: &mut DirHandle) {
        let self_inode = self.load_inode(ino).expect("can't load self inode");

        h.add(NameT {
            name: ".".to_string(),
            kind: Itype::Dir,
            ino,
        });
        h.add(NameT {
            name: "..".to_string(),
            kind: Itype::Dir,
            ino: if ino == 1 { 1 } else { self_inode.parent },
        });

        self.ensure_dir_index_loaded(ino);
        let entries = {
            let map = self.dir_index.lock().unwrap();
            map.get(&ino).map(|idx| idx.entries.clone()).unwrap_or_default()
        };
        for (name, ino) in entries {
            let inode = self.load_inode(ino).expect("can't load inode");
            let key = Dentry::key(self_inode.id, &name);
            self.dentry_cache_put(key, DentryCacheValue::Present(ino));
            h.add(NameT {
                name,
                kind: inode.kind,
                ino,
            });
        }
    }

    pub fn dentry_exist(&self, ino: Ino, name: impl AsRef<str>) -> bool {
        let n = name.as_ref();
        let key = Dentry::key(ino, n);
        match self.pending_get(&key) {
            PendingValue::Put(_) => true,
            PendingValue::Deleted => false,
            PendingValue::Missing => match self.dentry_cache_get(&key) {
                Some(DentryCacheValue::Present(_)) => true,
                Some(DentryCacheValue::Absent) => false,
                None => {
                    self.ensure_dir_index_loaded(ino);
                    match self.dir_index_lookup(ino, n) {
                        Some(Some(ino)) => {
                            self.dentry_cache_put(key, DentryCacheValue::Present(ino));
                            true
                        }
                        Some(None) => {
                            self.dentry_cache_put(key, DentryCacheValue::Absent);
                            false
                        }
                        None => self.meta.contains_key(&key).unwrap_or(false),
                    }
                }
            },
        }
    }

    /// if `key` exist, we can overwrite it
    pub fn store_dentry(&self, parent: Ino, name: impl AsRef<str>, ino: Ino) -> Result<(), String> {
        let key = Dentry::key(parent, name.as_ref());
        log::info!("store_dentry {}", key);
        let de = Dentry::new(parent, ino, name.as_ref());
        self.stage_put(key, de.val());
        Ok(())
    }

    pub fn delete_key(&self, key: &String) -> Result<(), String> {
        self.stage_del(key.clone());
        Ok(())
    }

    pub fn rename(
        &self,
        old_parent: Ino,
        old_name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), libc::c_int> {
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        let inode = self.lookup(old_parent, old_name).ok_or(ENOENT)?;

        if let Some(old_target_inode) = self.lookup(new_parent, new_name) {
            if old_target_inode.kind == Itype::Dir {
                if self.dir_has_entries(old_target_inode.id) {
                    return Err(ENOTEMPTY);
                }
            }
            // unlink target dentry and dec nlink
            self.unlink(new_parent, new_name)?;
        }

        let dkey = Dentry::key(old_parent, old_name);
        let new_dkey = Dentry::key(new_parent, new_name);
        let new_de = Dentry::new(new_parent, inode.id, new_name);

        self.stage_put(new_dkey, new_de.val());
        self.stage_del(dkey);

        if inode.kind == Itype::Dir && old_parent != new_parent {
            let mut inode = inode;
            inode.parent = new_parent;
            inode.ctime = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("can't get unix timestamp")
                .as_secs();
            self.stage_put(Inode::key(inode.id), inode.val());
            self.cache_put(inode, false);
        }

        Ok(())
    }

    pub fn link(&self, ino: Ino, new_parent: Ino, new_name: &str) -> Result<Inode, libc::c_int> {
        let mut inode = self.load_inode(ino).ok_or(ENOENT)?;
        if inode.kind == Itype::Dir {
            return Err(libc::EPERM); // hard links to directories are not allowed
        }

        if self.dentry_exist(new_parent, new_name) {
            return Err(EEXIST);
        }

        inode.links += 1;
        inode.ctime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("can't get unix timestamp")
            .as_secs();

        let dkey = Dentry::key(new_parent, new_name);
        let de = Dentry::new(new_parent, ino, new_name);
        self.stage_put(Inode::key(ino), inode.val());
        self.stage_put(dkey, de.val());

        self.cache_put(inode, false);
        Ok(inode)
    }
}
