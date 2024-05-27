use crate::meta::{Ino, MetaItem};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Dentry {
    parent: Ino,
    pub ino: Ino,
    pub name: String,
    size: usize, // TODO: calculate total size of directory
}

impl Dentry {
    pub fn new(parent: Ino, ino: Ino, name: &str) -> Self {
        Self {
            parent,
            ino,
            name: name.to_string(),
            size: 0,
        }
    }

    pub fn key(parent: Ino, name: &str) -> String {
        format!("d_{}_{}", parent, name)
    }

    pub fn val(this: &Self) -> Vec<u8> {
        bincode::serialize(this).expect("can't serialize dentry")
    }

    pub fn prefix(parent: Ino) -> String {
        format!("d_{}_", parent)
    }
}

impl MetaItem for Dentry {
    fn key(&self) -> String {
        Self::key(self.parent, &self.name)
    }

    fn val(&self) -> Vec<u8> {
        Self::val(self)
    }
}
