use crate::meta::inode::{Inode, Itype};
use crate::utils::{init_data_path, inode_key, sb_key};
use crate::utils::{BitMap, FS_TOTAL_INODES};
use libc::{EEXIST, EFAULT, ENOENT};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub type Ino = u64;

pub fn build_namei(parent: Ino, name: &String) -> String {
    format!("dentry_{parent}_{}", name)
}

/// namei format as `dentry_Ino_name`
pub fn extract_namei(key: &String) -> String {
    key.split(' ').nth(2).unwrap().to_string()
}

pub fn build_dentry_prefix(parent: Ino) -> String {
    format!("dentry_{parent}_")
}

pub struct NameI {
    pub name: String,
    pub inode: Inode,
}

// NOTE: we use a key-value database to store metadata of filesystem, so it's unnecessary to store
// inode map, data map and inode table in metadata, we only limit the total number of data blocks
// and inode count is enough
#[derive(Serialize, Deserialize, Debug)]
struct SuperBlock {
    name: String,
    uri: String, // currently the `uri` is a path to store file blocks
    imap: BitMap,
}

impl SuperBlock {
    fn new(name: String, uri: String) -> Self {
        SuperBlock {
            name,
            uri,
            imap: BitMap::new(FS_TOTAL_INODES as u64),
        }
    }

    fn alloc_ino(&mut self) -> Option<Ino> {
        self.imap.alloc()
    }

    fn free_ino(&mut self, ino: Ino) {
        self.imap.del(ino);
    }

    fn se(&self) -> Vec<u8> {
        bincode::serialize(self).expect("can't serialize SuperBlock")
    }
}

pub struct Meta {
    pub meta: sled::Db,
    sb: SuperBlock,
}

/// Meta Design
/// this struct impl most fs operations via a kv database including: superblock, inode, dentry
/// ### superblock
/// which hold meta info of total inode/data count and used count, the superblock is store as
/// key => `superblock` value => `SuperBlock`
/// ### inode
/// which hold meta info of a file, the most important member is `chunks`, the inode in kv database
/// has schema: key => `inode_Ino`, value => `chunks` list, the value maybe very large, but it's ok
/// since we are not for performance, we are to demonstration
/// ### dentry
/// it's a `name` to `inode number` map, and this maybe very large too, but we can limit member count
/// the `dentry`, since the database support range scan operation, we can simply format dentry to kv
/// store, with same prefix as hash key (in Redis) and the value is inode number, for example, let's
/// say a dentry's Ino is 3 and a file's Ino is 5 and it's name `foo`, then it should be a kv in db
/// like `dentry_3_foo` value is `5`, if there's another file name `bar` with Ino `6`, then another
/// key-value is `dentry_3_bar` -> `6`
/// ### NOTE: there's no cache support for inode and dentry at present
impl Meta {
    // write superblock
    pub fn format(meta_path: &String, store_path: &String) -> Result<(), String> {
        let db = sled::open(meta_path);
        if db.is_err() {
            return Err(db.err().unwrap().to_string());
        }

        let db = db.unwrap();
        let sb = SuperBlock::new("junkfs".to_string(), store_path.clone());
        let r = db.insert(sb_key(), sb.se());

        match r {
            Err(e) => Err(e.to_string()),
            Ok(_) => Ok(()),
        }
    }

    pub fn load(path: String) -> Result<Self, String> {
        let db = sled::open(path);
        if db.is_err() {
            return Err(db.err().unwrap().to_string());
        }
        let meta = db.unwrap();
        let sb = meta.get(sb_key()).expect("can't load superblock");
        match sb {
            None => Err("not formated".to_string()),
            Some(sb) => {
                let sb = bincode::deserialize::<SuperBlock>(&sb);

                match sb {
                    Err(e) => Err(e.to_string()),
                    Ok(sb) => {
                        init_data_path(sb.uri.clone());
                        Ok(Meta { meta, sb })
                    }
                }
            }
        }
    }

    pub fn close(&self) {
        self.meta.flush().unwrap();
    }

    pub fn flush_sb(&self) {
        let key = sb_key();
        self.meta.insert(key, self.sb.se()).expect("can't store kv");
    }

    /// - use `parent` and `name` to build dentry key
    /// - load value of dentry key
    /// - if existed, load Inode from database
    /// - or else, return None
    pub fn lookup(&mut self, parent: Ino, name: String) -> Option<Inode> {
        let parent = build_namei(parent, &name);
        match self.meta.get(&parent) {
            Err(e) => {
                eprintln!("can't load dentry {}, error {}", parent, e.to_string());
                None
            }
            Ok(dentry) => {
                let dentry = dentry.unwrap();
                let dentry = bincode::deserialize::<Ino>(&dentry);

                match dentry {
                    Err(e) => {
                        eprintln!("deserialize dentry fail, error {}", e.to_string());
                        return None;
                    }
                    Ok(inode) => {
                        return self.load_inode(inode);
                    }
                }
            }
        }
    }

    pub fn mknod(&mut self, parent: u64, name: String, mode: u32) -> Result<Inode, libc::c_int> {
        if self.dentry_exist(parent, &name) {
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
                kind: Itype::File,
                mode: mode as u16,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                atime: epoch,
                mtime: epoch,
                ctime: epoch,
                length: 0,
                links: 1,
                chunks: Vec::new(),
            };

            let r = self.store_inode(&inode);
            if r.is_err() {
                eprintln!("can't store inode {}", ino);
                self.sb.free_ino(ino);
                return Err(EFAULT);
            }

            let r = self.store_dentry(parent, &name, ino);
            if r.is_err() {
                self.sb.free_ino(ino);
                let key = inode_key(ino);
                self.delete_key(&key).expect("can't remove key");
                return Err(EFAULT);
            }

            self.flush_sb();
            Ok(inode)
        } else {
            Err(ENOENT)
        }
    }

    pub fn open(&mut self, ino: Ino, flags: i32) -> Option<Inode> {
        let inode = self.load_inode(ino);
        inode
    }

    pub fn load_inode(&self, inode: Ino) -> Option<Inode> {
        let key = inode_key(inode);
        match self.meta.get(&key) {
            Err(e) => {
                eprintln!("load inode error {}", e.to_string());
                return None;
            }
            Ok(tmp) => {
                if tmp.is_none() {
                    None
                } else {
                    let inode = bincode::deserialize::<Inode>(&tmp.unwrap());
                    if inode.is_err() {
                        eprintln!("deserialize inode fail error {}", inode.err().unwrap().to_string());
                        return None;
                    }
                    Some(inode.unwrap())
                }
            }
        }
    }

    /// if `key` exist, we can overwrite it
    pub fn store_inode(&mut self, inode: &Inode) -> Result<(), String> {
        let key = inode_key(inode.id);
        let r = self.meta.insert(key, inode.se());
        if r.is_err() {
            return Err(r.err().unwrap().to_string());
        }
        Ok(())
    }

    pub fn load_dentry(&self, ino: Ino) -> Option<Vec<NameI>> {
        let key = build_dentry_prefix(ino);
        let iter = self.meta.scan_prefix(&key);
        let mut r = vec![];
        for i in iter {
            match i {
                Err(e) => {
                    eprintln!("can't iterate over dentry {}", key);
                    return None;
                }
                Ok(x) => {
                    let (key, ino) = x;
                    let ino = bincode::deserialize::<Ino>(&ino).expect("can't deserialize inode");
                    let inode = self.load_inode(ino).expect("can't load inode");
                    let key = String::from_utf8_lossy(key.as_ref()).to_string();
                    let name = extract_namei(&key);
                    r.push(NameI { name, inode });
                }
            }
        }
        Some(r)
    }

    pub fn dentry_exist(&self, ino: Ino, name: &String) -> bool {
        let name = build_namei(ino, name);
        self.meta.contains_key(&name).expect("can't find key")
    }

    /// if `key` exist, we can overwrite it
    pub fn store_dentry(&mut self, parent: Ino, name: &String, ino: Ino) -> Result<(), String> {
        let key = build_namei(parent, name);
        if self.meta.contains_key(&key).is_err() {
            return Err(format!("key {key} exists"));
        }
        let se = bincode::serialize(&ino).expect("can't serialize Ino");
        let r = self.meta.insert(key, se);
        if r.is_err() {
            return Err(r.err().unwrap().to_string());
        }
        Ok(())
    }

    pub fn delete_key(&mut self, key: &String) -> Result<(), String> {
        let r = self.meta.remove(key);
        match r {
            Err(e) => {
                eprintln!("can't remove {}", key);
                Err(e.to_string())
            }
            Ok(_) => Ok(()),
        }
    }
}
