use crate::meta::{Ino, MetaKV};
use crate::utils::{FS_IMAP_GROUP_SIZE, FS_ROOT_INODE, FS_TOTAL_INODES};
use serde::{Deserialize, Serialize};

// NOTE: we use a key-value database to store metadata of filesystem, so it's unnecessary to store
// inode map, data map and inode table in metadata, we only limit the total number of data blocks
// and inode count is enough
// inode allocation bitmap is stored as separate imap_* keys to reduce write amplification
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SuperBlock {
    ino: Ino,
    uri: String, // currently the `uri` is a path to store file blocks
    version: u32,
    total_inodes: u64,
    group_size: u64,
    group_count: u64,
}

impl SuperBlock {
    pub fn new(uri: &str) -> Self {
        let total_inodes = FS_TOTAL_INODES;
        let group_size = FS_IMAP_GROUP_SIZE;
        assert!(group_size > 0);
        assert!(group_size % 64 == 0);
        let group_count = (total_inodes + group_size - 1) / group_size;
        SuperBlock {
            ino: FS_ROOT_INODE,
            uri: uri.to_string(),
            version: SUPERBLOCK_VERSION,
            total_inodes,
            group_size,
            group_count,
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn total_inodes(&self) -> u64 {
        self.total_inodes
    }

    pub fn group_size(&self) -> u64 {
        self.group_size
    }

    pub fn group_count(&self) -> u64 {
        self.group_count
    }

    pub fn check(&self) {
        assert_eq!(self.total_inodes, FS_TOTAL_INODES);
        assert!(self.group_size > 0);
        assert!(self.group_size % 64 == 0);
        assert_eq!(self.version, SUPERBLOCK_VERSION);
        let expect = (self.total_inodes + self.group_size - 1) / self.group_size;
        assert_eq!(self.group_count, expect);
    }

    pub fn version(&self) -> u32 {
        self.version
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

const SUPERBLOCK_VERSION: u32 = 3;

#[cfg(test)]
mod test {
    use crate::meta::kvstore::MaceStore;
    use crate::meta::super_block::SuperBlock;
    use crate::meta::MetaKV;
    use crate::utils::{FS_IMAP_GROUP_SIZE, FS_TOTAL_INODES};

    #[test]
    fn test_superblock() {
        let sb = SuperBlock::new("tmp");
        assert_eq!(sb.total_inodes(), FS_TOTAL_INODES);
        assert_eq!(sb.group_size(), FS_IMAP_GROUP_SIZE);

        // let tmp = SuperBlock::val(&sb);
        let tmp = sb.val();

        let bs = bincode::deserialize::<SuperBlock>(tmp.as_slice()).unwrap();

        assert_eq!(bs.total_inodes(), sb.total_inodes());
        assert_eq!(bs.group_size(), sb.group_size());
        assert_eq!(bs.group_count(), sb.group_count());
        assert_eq!(bs.version(), sb.version());

        let path = "/tmp/test_sb";
        let _ = std::fs::remove_dir_all(path);
        let _ = std::fs::create_dir_all(path);
        let db = MaceStore::new(path);

        let x = db.insert("sb", &tmp);
        println!("{:?}, tmp.len {:?}", x, tmp.len());

        let tmp = db.get("sb").unwrap();
        let bs = bincode::deserialize::<SuperBlock>(tmp.as_ref()).unwrap();

        assert_eq!(bs.total_inodes(), sb.total_inodes());
        assert_eq!(bs.group_size(), sb.group_size());
        assert_eq!(bs.group_count(), sb.group_count());
    }
}
