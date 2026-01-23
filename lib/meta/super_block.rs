use crate::meta::{Ino, MetaKV};
use crate::utils::{BitMap, FS_ROOT_INODE, FS_TOTAL_INODES};
use serde::{Deserialize, Serialize};

// NOTE: we use a key-value database to store metadata of filesystem, so it's unnecessary to store
// inode map, data map and inode table in metadata, we only limit the total number of data blocks
// and inode count is enough
#[derive(Serialize, Deserialize, Debug)]
pub struct SuperBlock {
    ino: Ino,
    uri: String, // currently the `uri` is a path to store file blocks
    imap: BitMap,
}

impl SuperBlock {
    pub fn new(uri: &str) -> Self {
        let mut imap = BitMap::new(FS_TOTAL_INODES);
        // reserve ino 0
        imap.add(0);
        SuperBlock {
            ino: FS_ROOT_INODE,
            uri: uri.to_string(),
            imap,
        }
    }

    pub fn alloc_ino(&mut self) -> Option<Ino> {
        self.imap.alloc()
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn check(&self) {
        assert_eq!(self.imap.cap(), FS_TOTAL_INODES);
        let mut cnt = 0;

        for i in 0..FS_TOTAL_INODES {
            if self.imap.test(i) {
                cnt += 1;
            }
        }
        assert_eq!(cnt, self.imap.len());
    }

    pub fn free_ino(&mut self, ino: Ino) {
        if self.imap.test(ino) {
            self.imap.del(ino);
        } else {
            log::error!("non existed ino {}", ino);
        }
    }

    pub fn key() -> String {
        "sb".to_string()
    }

    pub fn val(this: &Self) -> Vec<u8> {
        bincode::serialize(this).expect("can't serialize superblock")
    }
}

impl MetaKV for SuperBlock {
    fn key(&self) -> String {
        Self::key()
    }

    fn val(&self) -> Vec<u8> {
        Self::val(self)
    }
}

#[cfg(test)]
mod test {
    use crate::meta::kvstore::MaceStore;
    use crate::meta::super_block::SuperBlock;
    use crate::meta::MetaKV;

    #[test]
    fn test_superblock() {
        let mut sb = SuperBlock::new("tmp");

        sb.alloc_ino();
        sb.alloc_ino();
        sb.alloc_ino();

        assert_eq!(sb.imap.len(), 3);

        // let tmp = SuperBlock::val(&sb);
        let tmp = sb.val();

        let bs = bincode::deserialize::<SuperBlock>(tmp.as_slice()).unwrap();

        assert_eq!(bs.imap.len(), 3);

        let path = "/tmp/test_sb";
        let _ = std::fs::remove_dir_all(path);
        let _ = std::fs::create_dir_all(path);
        let db = MaceStore::new(path);

        let x = db.insert("sb", &tmp);
        println!("{:?}, tmp.len {:?}", x, tmp.len());

        let tmp = db.get("sb").unwrap();
        let bs = bincode::deserialize::<SuperBlock>(tmp.as_ref()).unwrap();

        assert_eq!(bs.imap.len(), sb.imap.len());
        assert!(bs.imap.test(0));
        assert!(bs.imap.test(1));
        assert!(bs.imap.test(2));
    }
}
