use serde::{Deserialize, Serialize};

// NOTE: we use a key-value database to store metadata of filesystem, so it's unnecessary to store
// inode map, data map and inode table in metadata, we only limit the total number of data blocks
// and inode count is enough
#[derive(Serialize, Deserialize, Debug)]
pub struct SuperBlock {
    name: String,
    total_inode: u32,
    total_data: u32,
    used_inode: u32,
    used_data: u32,
}

impl SuperBlock {
    pub fn new(name: String, total_inode: u32, total_data: u32) -> Self {
        SuperBlock {
            name,
            total_inode,
            total_data,
            used_inode: 0,
            used_data: 0,
        }
    }
}
pub fn hello() {
    println!("hello from lib/sb");
}
