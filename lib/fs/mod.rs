mod file;
mod inode;
pub mod namei;
pub mod superblock;

use file::FileHandle;
pub use inode::{Chunk, Ino, Inode, Slice};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};
pub use superblock::SuperBlock;

pub const CHUNK_SIZE: u64 = 1 << 26;
pub const BLOCK_SIZE: u64 = 1 << 22;
pub const FS_BLK_SIZE: u64 = 4096;

use crate::fs::inode::Itype;
use crate::fs::namei::{build_dentry_prefix, build_namei, extract_namei, NameI};
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::{EACCES, EEXIST};

pub fn to_systime(s: u64) -> SystemTime {
    UNIX_EPOCH + time::Duration::from_secs(s)
}

pub fn to_filetype(s: Itype) -> FileType {
    match s {
        Itype::File => FileType::RegularFile,
        Itype::Link => FileType::Symlink,
        Itype::Dir => FileType::Directory,
    }
}

pub fn to_attr(inode: Inode) -> FileAttr {
    FileAttr {
        ino: inode.id,
        size: inode.length,
        blocks: inode.blocks(),
        atime: to_systime(inode.atime),
        mtime: to_systime(inode.mtime),
        ctime: to_systime(inode.ctime),
        kind: to_filetype(inode.kind),
        perm: inode.mode,
        nlink: inode.links,
        uid: inode.uid,
        gid: inode.gid,
        blksize: FS_BLK_SIZE as u32,
        // the following is unused
        rdev: 0,
        crtime: time::SystemTime::now(),
        flags: 0,
    }
}

pub fn dentry_key(inode: Ino) -> String {
    format!("dentry_{inode}")
}

pub fn inode_key(inode: Ino) -> String {
    format!("inode_{inode}")
}

struct Meta {
    meta: sled::Db,
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
    fn new(path: String) -> Result<Self, String> {
        let db = sled::open(path);
        if db.is_err() {
            return Err(db.err().unwrap().to_string());
        }
        Ok(Meta { meta: db.unwrap() })
    }

    fn close(&self) {
        self.meta.flush().unwrap();
    }

    /// - use `parent` and `name` to build dentry key
    /// - load value of dentry key
    /// - if existed, load Inode from database
    /// - or else, return None
    fn lookup(&mut self, parent: Ino, name: String) -> Option<Inode> {
        let parent = dentry_key(parent);
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

    fn load_inode(&self, inode: Ino) -> Option<Inode> {
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
                        eprintln!(
                            "deserialize inode fail error {}",
                            inode.err().unwrap().to_string()
                        );
                        return None;
                    }
                    Some(inode.unwrap())
                }
            }
        }
    }

    fn store_inode(&mut self, inode: Inode) -> Result<(), String> {
        let key = inode_key(inode.id);
        let r = self.meta.insert(key, inode.se());
        if r.is_err() {
            return Err(r.err().unwrap().to_string());
        }
        Ok(())
    }

    fn load_dentry(&self, ino: Ino) -> Option<Vec<NameI>> {
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

    fn store_dentry(&mut self, parent: Ino, name: &String, ino: Ino) -> Result<(), String> {
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
}

pub struct Fs {
    meta: Meta,
    mount_point: String,
    file_handle: HashMap<Ino, Vec<FileHandle>>,
    file_counter: u64,
}

impl Fs {
    pub fn new(path: String, mp: String) -> Result<Self, String> {
        let meta = Meta::new(path);
        if meta.is_err() {
            return Err(meta.err().unwrap());
        }

        Ok(Fs {
            meta: meta.unwrap(),
            mount_point: mp,
            file_handle: HashMap::new(),
            file_counter: 0,
        })
    }

    pub fn mount(&self) -> Result<(), String> {
        todo!()
    }

    fn new_fid(&mut self) -> u64 {
        let mut r = 0;
        if self.file_handle.len() >= self.file_counter as usize {
            self.file_counter += 1;
            r = self.file_counter;
        } else {
            for i in 1..self.file_counter {
                if !self.file_handle.contains_key(&i) {
                    r = i;
                    break;
                }
            }
        }
        return r;
    }

    fn find_handle(&mut self, ino: Ino, fh: u64) -> Option<&mut FileHandle> {
        let v = self.file_handle.get_mut(&ino);
        if v.is_none() {
            return None;
        }

        let v = v.unwrap();
        for i in v {
            if i.handle == fh {
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
            let attr = to_attr(inode);
            let ttl = time::Duration::new(1, 0);
            reply.entry(&ttl, &attr, 0);
        } else {
            reply.error(libc::EEXIST);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        todo!()
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
        todo!()
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
                let buf = file.read(offset, size);
                reply.data(&buf);
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

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
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
        todo!()
    }
}

impl Drop for Fs {
    fn drop(&mut self) {
        self.meta.close()
    }
}
