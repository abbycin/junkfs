use crate::meta::dentry::Dentry;
use crate::meta::inode::{Inode, Itype};
use crate::meta::kvstore::MaceStore;
use crate::meta::super_block::SuperBlock;
use crate::meta::{DirHandle, MetaKV};
use crate::utils::init_data_path;
use libc::{EEXIST, EFAULT, ENOENT, ENOTEMPTY};
use mace::{Mace, OpCode, Options};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub type Ino = u64;

pub struct NameT {
    pub name: String,
    pub kind: Itype,
    pub ino: Ino,
}

pub struct Meta {
    pub meta: MaceStore,
    sb: SuperBlock,
}

impl Meta {
    // write superblock
    pub fn format(meta_path: &str, store_path: &str) -> Result<(), String> {
        let mut opt = Options::new(meta_path);
        opt.concurrent_write = 1;
        let db = Mace::new(opt.validate().unwrap());
        if db.is_err() {
            return Err(format!("{:?}", db.err()));
        }

        let db = db.unwrap();
        let mut sb = SuperBlock::new(store_path);
        
        let kv = db.begin().unwrap();
        
        // alloc root inode id (1)
        let root_ino = sb.alloc_ino().expect("can't alloc root ino");
        assert_eq!(root_ino, 1);

        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

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

        kv.put(SuperBlock::key(), sb.val()).unwrap();
        kv.put(Inode::key(root_ino), Inode::val(&root_inode)).unwrap();
        
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
                        init_data_path(sb.uri());
                        Ok(Meta { meta, sb })
                    }
                }
            }
        }
    }

    pub fn store(&mut self, key: &str, value: &[u8]) {
        match self.meta.insert(key, value) {
            Ok(_) => {}
            Err(e) => {
                log::info!("insert key {} fail, error {:?}", key, e)
            }
        }
    }

    pub fn load(&self, key: &str) -> Option<Vec<u8>> {
        match self.meta.get(key) {
            Err(e) => {
                log::error!("can't load key {} error {}", key, e);
                None
            }
            Ok(x) => Some(x),
        }
    }

    pub fn close(&mut self) {}

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
    pub fn lookup(&mut self, parent: Ino, name: &str) -> Option<Inode> {
        let parent = Dentry::key(parent, name);
        match self.meta.get(&parent) {
            Err(e) => {
                log::error!("can't load dentry {}, error {}", parent, e);
                None
            }
            Ok(dentry) => {
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

        if let Some(ino) = self.sb.alloc_ino() {
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

            let _ = self.flush_sb();
            Ok(inode)
        } else {
            Err(ENOENT)
        }
    }

    pub fn unlink(&mut self, parent: Ino, name: &str) -> Result<Inode, libc::c_int> {
        let key = self.lookup(parent, name);

        if key.is_none() {
            return Err(ENOENT);
        }
        let inode = key.unwrap();
        if inode.kind == Itype::Dir {
            let prefix = Dentry::prefix(inode.id);
            let view = self.meta.view();
            let mut it = view.seek(&prefix);
            if it.next().is_some() {
                return Err(ENOTEMPTY);
            }
        }
        let ikey = Inode::key(inode.id);
        let dkey = Dentry::key(parent, name);
        self.delete_key(&ikey).unwrap();
        self.delete_key(&dkey).unwrap();
        self.sb.free_ino(inode.id);
        let _ = self.flush_sb();
        Ok(inode)
    }

    pub fn load_inode(&self, inode: Ino) -> Option<Inode> {
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
                Some(inode.unwrap())
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

    pub fn load_dentry(&self, ino: Ino, h: &mut DirHandle) {
        let key = Dentry::prefix(ino);
        let view = self.meta.view();
        let iter = view.seek(&key);

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

        for item in iter {
            if !item.key().starts_with(key.as_bytes()) {
                break;
            }
            let de = bincode::deserialize::<Dentry>(item.val()).expect("can't deserialize dentry");
            let inode = self.load_inode(de.ino).expect("can't load inode");
            h.add(NameT {
                name: de.name,
                kind: inode.kind,
                ino: de.ino,
            });
        }
    }

    pub fn dentry_exist(&self, ino: Ino, name: impl AsRef<str>) -> bool {
        let name = Dentry::key(ino, name.as_ref());
        self.meta.contains_key(&name).unwrap_or(false)
    }

    /// if `key` exist, we can overwrite it
    pub fn store_dentry(&mut self, parent: Ino, name: impl AsRef<str>, ino: Ino) -> Result<(), String> {
        let key = Dentry::key(parent, name.as_ref());
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
                log::error!("can't remove {} error {:?}", key, e);
                Err(format!("{:?}", e))
            }
            Ok(_) => Ok(()),
        }
    }

    pub fn rename(
        &mut self,
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
                // check if empty
                let prefix = Dentry::prefix(old_target_inode.id);
                let view = self.meta.view();
                let mut it = view.seek(&prefix);
                if it.next().is_some() {
                    return Err(ENOTEMPTY);
                }
            }
            // unlink target dentry and dec nlink
            self.unlink(new_parent, new_name)?;
        }

        // store new dentry
        self.store_dentry(new_parent, new_name, inode.id).map_err(|_| EFAULT)?;

        // remove old dentry
        let dkey = Dentry::key(old_parent, old_name);
        self.delete_key(&dkey).map_err(|_| EFAULT)?;

        Ok(())
    }

    pub fn link(
        &mut self,
        ino: Ino,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<Inode, libc::c_int> {
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

        self.store_inode(&inode).map_err(|_| EFAULT)?;
        self.store_dentry(new_parent, new_name, ino).map_err(|_| EFAULT)?;

        Ok(inode)
    }
}
