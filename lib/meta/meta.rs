use crate::meta::dentry::Dentry;
use crate::meta::inode::{Inode, Itype};
use crate::meta::sled::SledStore;
use crate::meta::super_block::SuperBlock;
use crate::meta::{DirHandle, MetaKV, MetaStore};
use crate::utils::{init_data_path, FS_META_CACHE_SIZE};
use libc::{EEXIST, EFAULT, ENOENT, ENOTEMPTY};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Ino = u64;

pub struct NameT {
    pub name: String,
    pub kind: Itype,
}

pub struct Meta {
    pub meta: Box<dyn MetaStore>,
    sb: SuperBlock,
}

impl Meta {
    // write superblock
    pub fn format(meta_path: &str, store_path: &str) -> Result<(), String> {
        let db = sled::open(meta_path);
        if db.is_err() {
            return Err(db.err().unwrap().to_string());
        }

        let db = db.unwrap();
        let sb = SuperBlock::new(store_path);
        let r = db.insert(SuperBlock::key(), sb.val());

        match r {
            Err(e) => Err(e.to_string()),
            Ok(_) => Ok(()),
        }
    }

    pub fn load_fs(path: String) -> Result<Self, String> {
        let meta = Box::new(SledStore::new(&path, FS_META_CACHE_SIZE));
        let sb = meta.get(&SuperBlock::key());
        match sb {
            Err(e) => Err(e),
            Ok(sb) => match sb {
                None => Err("not formated".to_string()),
                Some(sb) => {
                    let sb = bincode::deserialize::<SuperBlock>(&sb);

                    match sb {
                        Err(e) => Err(e.to_string()),
                        Ok(sb) => {
                            init_data_path(sb.uri());
                            Ok(Meta { meta, sb })
                        }
                    }
                }
            },
        }
    }

    pub fn store(&mut self, key: &str, value: &[u8]) {
        match self.meta.insert(key, value) {
            Ok(_) => {}
            Err(e) => {
                log::info!("insert key {} fail, error {}", key, e.to_string())
            }
        }
    }

    pub fn load(&self, key: &str) -> Option<Vec<u8>> {
        match self.meta.get(key) {
            Err(e) => {
                log::error!("can't load key {} error {}", key, e);
                None
            }
            Ok(x) => x,
        }
    }

    pub fn close(&mut self) {
        self.meta.flush();
    }

    pub fn flush_sb(&self) -> Result<(), String> {
        match self.meta.insert(&SuperBlock::key(), &self.sb.val()) {
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
    pub fn lookup(&mut self, parent: Ino, name: &String) -> Option<Inode> {
        let parent = Dentry::key(parent, &name);
        match self.meta.get(&parent) {
            Err(e) => {
                log::error!("can't load dentry {}, error {}", parent, e.to_string());
                None
            }
            Ok(dentry) => {
                if dentry.is_none() {
                    log::info!("can't find dentry {}", parent);
                    return None;
                }
                let dentry = dentry.unwrap();
                let dentry = bincode::deserialize::<Dentry>(&dentry).expect("can't deserialize dentry");
                self.load_inode(dentry.ino)
            }
        }
    }

    pub fn mknod(&mut self, parent: u64, name: impl AsRef<str>, ftype: Itype, mode: u32) -> Result<Inode, libc::c_int> {
        if self.dentry_exist(parent, name.as_ref()) {
            log::error!("node existed dentry {}", Dentry::key(parent, name.as_ref()));
            return Err(EEXIST);
        }

        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("can't get unix timestamp")
            .as_secs();

        // NOTE: for superblock, we skip slot 0 in bitmap
        if parent == 0 {
            self.sb.alloc_ino().unwrap();
        }
        if let Some(ino) = self.sb.alloc_ino() {
            if parent == 0 {
                assert_eq!(ino, 1);
            }
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

            let r = self.store_inode(&inode);
            if r.is_err() {
                log::error!("can't store inode {}", ino);
                self.sb.free_ino(ino);
                return Err(EFAULT);
            }

            let r = self.store_dentry(parent, &name, ino);
            if r.is_err() {
                self.sb.free_ino(ino);
                let key = Inode::key(ino);
                self.delete_key(&key).expect("can't remove key");
                return Err(EFAULT);
            }

            Ok(inode)
        } else {
            Err(ENOENT)
        }
    }

    pub fn unlink(&mut self, parent: Ino, name: &String) -> Result<Inode, libc::c_int> {
        let key = self.lookup(parent, &name);

        if key.is_none() {
            return Err(ENOENT);
        }
        let inode = key.unwrap();
        if inode.kind == Itype::Dir {
            let prefix = Dentry::prefix(inode.id);
            let mut it = self.meta.scan_prefix(&prefix);
            if it.next().is_some() {
                return Err(ENOTEMPTY);
            }
        }
        let ikey = Inode::key(inode.id);
        let dkey = Dentry::key(parent, name);
        self.delete_key(&ikey).unwrap();
        self.delete_key(&dkey).unwrap();
        self.sb.free_ino(inode.id);
        Ok(inode)
    }

    pub fn load_inode(&self, inode: Ino) -> Option<Inode> {
        let key = Inode::key(inode);
        match self.meta.get(&key) {
            Err(e) => {
                log::error!("load inode error {}", e.to_string());
                return None;
            }
            Ok(tmp) => {
                if tmp.is_none() {
                    log::error!("can't find inode {}", key);
                    None
                } else {
                    let inode = bincode::deserialize::<Inode>(&tmp.unwrap());
                    if inode.is_err() {
                        log::error!("deserialize inode fail error {}", inode.err().unwrap().to_string());
                        return None;
                    }
                    Some(inode.unwrap())
                }
            }
        }
    }

    /// if `key` exist, we can overwrite it
    pub fn store_inode(&mut self, inode: &Inode) -> Result<(), String> {
        let key = Inode::key(inode.id);
        let r = self.meta.insert(&key, &inode.val());
        if r.is_err() {
            return Err(r.err().unwrap().to_string());
        }
        Ok(())
    }

    pub fn load_dentry(&self, ino: Ino, handle: &Rc<RefCell<DirHandle>>) {
        let key = Dentry::prefix(ino);
        let mut iter = self.meta.scan_prefix(&key);

        handle.borrow_mut().add(NameT {
            name: ".".to_string(),
            kind: Itype::Dir,
        });
        handle.borrow_mut().add(NameT {
            name: "..".to_string(),
            kind: Itype::Dir,
        });

        while let Some(i) = iter.next() {
            let de = bincode::deserialize::<Dentry>(&i).expect("can't deserialize dentry");
            let inode = self.load_inode(de.ino).expect("can't load inode");
            handle.borrow_mut().add(NameT {
                name: de.name,
                kind: inode.kind,
            });
        }
    }

    pub fn dentry_exist(&self, ino: Ino, name: impl AsRef<str>) -> bool {
        let name = Dentry::key(ino, name.as_ref());
        self.meta.contains_key(&name).expect("can't find key")
    }

    /// if `key` exist, we can overwrite it
    pub fn store_dentry(&mut self, parent: Ino, name: impl AsRef<str>, ino: Ino) -> Result<(), String> {
        let key = Dentry::key(parent, name.as_ref());
        if self.meta.contains_key(&key).is_err() {
            log::error!("dentry existed {}", key);
            return Err(format!("key {key} exists"));
        }
        log::info!("store_dentry {}", key);
        let de = Dentry::new(parent, ino, name.as_ref());
        let r = self.meta.insert(&key, &de.val());
        if r.is_err() {
            log::error!("insert key {} vaule {} fail", key, ino);
            return Err(r.err().unwrap().to_string());
        }
        Ok(())
    }

    pub fn delete_key(&mut self, key: &String) -> Result<(), String> {
        let r = self.meta.remove(key);
        match r {
            Err(e) => {
                log::error!("can't remove {} error {}", key, e);
                Err(e.to_string())
            }
            Ok(_) => Ok(()),
        }
    }
}
