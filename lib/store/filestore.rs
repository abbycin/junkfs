use crate::meta::Ino;
use crate::utils::{get_data_path, FS_BLK_SIZE};
use std::collections::HashMap;
use std::os::unix::prelude::FileExt;

/// `FileStore` provide interface to access fixed size block on disk, it will create block when necessary
/// and the block is always locate in the form `location/ino/block_idx_off_size` where `block_idx`
/// is `pos / FS_BLK_SIZE` and `off` is `pos % FS_BLK_SIZE` which is in block offset, and the `size`
/// is one `write_at` size
/// **NOTE: no cache support at present!!**
pub struct FileStore {
    pub fh: u64,
    ofs: HashMap<String, std::fs::File>, // key => `ino-blk-off-size`
}

impl FileStore {
    fn build_dir(&self, ino: Ino) -> String {
        format!("{}/{ino}", get_data_path())
    }

    fn build_path(&self, ino: Ino, index: u64, off: u64, size: usize) -> String {
        format!("{}/{}/{}_{}_{}", get_data_path(), ino, index, off, size)
    }

    fn build_key(&self, ino: Ino, blk: u64, off: u64, size: usize) -> String {
        format!("{ino}-{blk}-{off}-{size}")
    }

    fn extract_pos(&self, pos: u64, size: usize) -> (u64, u64, usize) {
        let blk_idx = pos / FS_BLK_SIZE;
        let in_blk_off = pos % FS_BLK_SIZE;
        let mut len = (FS_BLK_SIZE - pos) as usize;

        if len > size {
            len = size;
        }

        (blk_idx, in_blk_off, len)
    }

    fn open_file(&mut self, ino: Ino, index: u64, off: u64, size: usize) -> Option<&mut std::fs::File> {
        let key = self.build_key(ino, index, off, size);
        if self.ofs.contains_key(&key) {
            return self.ofs.get_mut(&key);
        }
        let r = std::fs::create_dir_all(&self.build_dir(ino)); // ignore result
        if r.is_err() {
            eprint!("can't create dir {} errno {}", self.build_dir(ino), r.err().unwrap());
            return None;
        }
        let fpath = self.build_path(ino, index, off, size);

        // TODO: we need to check last current `p` has same prefix to `fpath` namely, same bock, maybe
        // same pos, but differnect size, or different pos and different size
        // for first case, we can return rest space to FS_BLK_SIZE to write, and split a write into
        // two write, first write fill rest space of a old block, second write create new block
        // for second case, if new pos small than or equal to old pos, then do as first case, or else check new
        // size and old size, if old_pos + old_size < new_pos, fill zero to range [old_pos + old_size, new_pos - old_size]
        // or else do as first case, overwrite old data
        // NOTE: we need rename the old block to new size (total size including zeroes range)
        match std::fs::File::create(&fpath) {
            Err(e) => {
                eprintln!("can't crate file {}", fpath);
                None
            }
            Ok(file) => {
                self.ofs.insert(key.clone(), file);
                self.ofs.get_mut(&key)
            }
        }
    }

    pub fn new(fh: u64) -> Self {
        Self {
            fh,
            ofs: HashMap::new(),
        }
    }

    // write at most one block a time
    fn write_block(&mut self, ino: Ino, pos: u64, data: &[u8], size: usize) -> std::io::Result<usize> {
        let (index, off, len) = self.extract_pos(pos, size);
        let f = self.open_file(ino, index, off, len);
        if f.is_none() {
            let e = std::io::Error::from(std::io::ErrorKind::NotFound);
            return Err(e);
        }
        let f = f.unwrap();
        let len = len as usize;
        let off = off as usize;

        let buf = &data[off..len];
        let r = f.write_at(buf, off as u64)?;
        assert_eq!(r, len);
        // the len is either FS_BLOCK_SIZE - off or data.len()
        Ok(len)
    }

    /// - `pos` is global offset in file
    /// - `data` is current buffer to write
    pub fn write_at(&mut self, ino: Ino, pos: u64, data: &[u8]) -> std::io::Result<usize> {
        let size = data.len();
        let mut pos = pos;
        let mut data = data;
        let mut cnt = 0;

        while cnt < size {
            let r = self.write_block(ino, pos, data, size - cnt)?;
            pos += r as u64;
            data = &data[r..];
            cnt += r;
        }
        assert_eq!(cnt, size);
        Ok(cnt)
    }

    /// - `pos` is global offset in file
    /// - `size` read at most one block
    pub fn read_at(&mut self, ino: Ino, pos: u64, size: u64) -> std::io::Result<Vec<u8>> {
        let (index, off, size) = self.extract_pos(pos, size as usize);
        let p = self.build_path(ino, index, off, size);
        let fpath = std::path::Path::new(&p);

        // check file exist, if not return error
        if !fpath.exists() {
            return Err(std::io::Error::from(std::io::ErrorKind::NotFound));
        }

        let file = self.open_file(ino, index, off, size);
        if file.is_none() {
            return Err(std::io::Error::from(std::io::ErrorKind::NotFound));
        }

        let file = file.unwrap();
        let mut v = vec![0u8; size];
        let mut buf = v.as_mut_slice();

        match file.read_at(&mut buf, off) {
            Err(e) => Err(e),
            Ok(n) => {
                assert_eq!(n, size);
                Ok(v)
            }
        }
    }
}
