use crate::fs::namei::NameI;
use crate::fs::Ino;

pub struct FileHandle {
    pub ino: Ino,
    pub handle: u64, // file descriptor
    pub off: u64,    // current offset in file or cursor when list directory
}

impl FileHandle {
    pub fn read(&self, offset: i64, size: u32) -> Vec<u8> {
        vec![]
    }
}
