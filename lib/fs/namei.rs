use crate::fs::{Ino, Inode};

pub struct NameI {
    pub name: String,
    pub inode: Inode,
}

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
