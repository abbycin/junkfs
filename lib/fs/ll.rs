use crate::fs::Fs;
use crate::meta::{Inode, Itype};
use crate::store::FileStore;
use crate::utils::FS_BLK_SIZE;
use junkfs_fuse as fuse;
use libc::{EFAULT, EIO, ENOENT};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::time::{SystemTime, UNIX_EPOCH};

const TTL_SEC: f64 = 5.0;
const NEG_TTL_SEC: f64 = 5.0;

unsafe fn fs_from_req(req: fuse::fuse_req_t) -> &'static Fs {
    let ud = fuse::fuse_req_userdata(req) as *mut Fs;
    &*ud
}

unsafe fn reply_err(req: fuse::fuse_req_t, err: i32) {
    let _ = fuse::fuse_reply_err(req, err);
}

unsafe fn reply_negative(req: fuse::fuse_req_t) {
    let mut e: fuse::fuse_entry_param = std::mem::zeroed();
    e.ino = 0;
    e.attr_timeout = 0.0;
    e.entry_timeout = NEG_TTL_SEC;
    let _ = fuse::fuse_reply_entry(req, &mut e);
}

fn kind_to_mode(kind: Itype) -> libc::mode_t {
    match kind {
        Itype::File => libc::S_IFREG,
        Itype::Dir => libc::S_IFDIR,
        Itype::Symlink => libc::S_IFLNK,
    }
}

fn inode_to_stat(inode: &Inode) -> fuse::stat {
    let mut st: fuse::stat = unsafe { std::mem::zeroed() };
    st.st_ino = inode.id as fuse::fuse_ino_t;
    st.st_mode = kind_to_mode(inode.kind) | inode.mode as libc::mode_t;
    st.st_nlink = inode.links as libc::nlink_t;
    st.st_uid = inode.uid as libc::uid_t;
    st.st_gid = inode.gid as libc::gid_t;
    st.st_size = inode.length as libc::off_t;
    st.st_blksize = FS_BLK_SIZE as libc::blksize_t;
    st.st_blocks = inode.blocks() as libc::blkcnt_t;
    st.st_atim.tv_sec = inode.atime as libc::time_t;
    st.st_atim.tv_nsec = 0;
    st.st_mtim.tv_sec = inode.mtime as libc::time_t;
    st.st_mtim.tv_nsec = 0;
    st.st_ctim.tv_sec = inode.ctime as libc::time_t;
    st.st_ctim.tv_nsec = 0;
    st
}

fn inode_to_entry(inode: &Inode) -> fuse::fuse_entry_param {
    let mut e: fuse::fuse_entry_param = unsafe { std::mem::zeroed() };
    e.ino = inode.id as fuse::fuse_ino_t;
    e.generation = 1;
    e.attr = inode_to_stat(inode);
    e.attr_timeout = TTL_SEC;
    e.entry_timeout = TTL_SEC;
    e
}

#[no_mangle]
pub extern "C" fn junkfs_ll_init(userdata: *mut c_void, conn: *mut fuse::fuse_conn_info) {
    let _ = userdata;
    if conn.is_null() {
        return;
    }
    unsafe {
        (*conn).max_write = 16 << 20;
        (*conn).max_read = 16 << 20;
        (*conn).max_readahead = 16 << 20;
        (*conn).want |= fuse::FUSE_CAP_ASYNC_READ as u32;
        (*conn).want |= fuse::FUSE_CAP_WRITEBACK_CACHE as u32;
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_destroy(userdata: *mut c_void) {
    let _ = userdata;
}

#[no_mangle]
pub extern "C" fn junkfs_ll_lookup(req: fuse::fuse_req_t, parent: fuse::fuse_ino_t, name: *const c_char) {
    if name.is_null() {
        unsafe { reply_err(req, ENOENT) };
        return;
    }
    let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().lookup(parent as u64, &name) {
        Some(inode) => {
            let mut e = inode_to_entry(&inode);
            unsafe { fuse::fuse_reply_entry(req, &mut e) };
        }
        None => unsafe { reply_negative(req) },
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_getattr(req: fuse::fuse_req_t, ino: fuse::fuse_ino_t, _fi: *mut fuse::fuse_file_info) {
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().load_inode(ino as u64) {
        Some(inode) => {
            let st = inode_to_stat(&inode);
            unsafe { fuse::fuse_reply_attr(req, &st, TTL_SEC) };
        }
        None => unsafe { reply_err(req, ENOENT) },
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_setattr(
    req: fuse::fuse_req_t,
    ino: fuse::fuse_ino_t,
    attr: *mut fuse::stat,
    to_set: i32,
    _fi: *mut fuse::fuse_file_info,
) {
    let fs = unsafe { fs_from_req(req) };
    let meta = fs.meta();
    let attr = unsafe {
        if attr.is_null() {
            reply_err(req, EFAULT);
            return;
        }
        &*attr
    };
    match meta.load_inode(ino as u64) {
        None => unsafe { reply_err(req, ENOENT) },
        Some(mut inode) => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if to_set & fuse::FUSE_SET_ATTR_MODE as i32 != 0 {
                inode.mode = (attr.st_mode & 0o7777) as u16;
            }
            if to_set & fuse::FUSE_SET_ATTR_UID as i32 != 0 {
                inode.uid = attr.st_uid as u32;
            }
            if to_set & fuse::FUSE_SET_ATTR_GID as i32 != 0 {
                inode.gid = attr.st_gid as u32;
            }
            if to_set & fuse::FUSE_SET_ATTR_SIZE as i32 != 0 {
                let size = attr.st_size as u64;
                if size < inode.length {
                    fs.flush_open_file_handles(ino as u64);
                }
                if let Err(e) = FileStore::set_len(ino as u64, size) {
                    log::error!("can't set_len ino {} size {} error {}", ino, size, e);
                    unsafe { reply_err(req, EFAULT) };
                    return;
                }
                if size != inode.length {
                    inode.mtime = now;
                    inode.ctime = now;
                }
                inode.length = size;
            }
            if to_set & fuse::FUSE_SET_ATTR_ATIME as i32 != 0 {
                inode.atime = attr.st_atim.tv_sec as u64;
            }
            if to_set & fuse::FUSE_SET_ATTR_MTIME as i32 != 0 {
                inode.mtime = attr.st_mtim.tv_sec as u64;
            }
            if to_set & fuse::FUSE_SET_ATTR_ATIME_NOW as i32 != 0 {
                inode.atime = now;
            }
            if to_set & fuse::FUSE_SET_ATTR_MTIME_NOW as i32 != 0 {
                inode.mtime = now;
            }
            if let Err(e) = meta.store_inode(&inode) {
                log::error!("can't store inode {} error {}", ino, e);
                unsafe { reply_err(req, EFAULT) };
                return;
            }
            let st = inode_to_stat(&inode);
            unsafe { fuse::fuse_reply_attr(req, &st, TTL_SEC) };
        }
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_mknod(
    req: fuse::fuse_req_t,
    parent: fuse::fuse_ino_t,
    name: *const c_char,
    mode: libc::mode_t,
    _rdev: libc::dev_t,
) {
    if name.is_null() {
        unsafe { reply_err(req, ENOENT) };
        return;
    }
    let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().mknod(parent as u64, &name, Itype::File, mode as u32) {
        Ok(inode) => {
            let mut e = inode_to_entry(&inode);
            unsafe { fuse::fuse_reply_entry(req, &mut e) };
        }
        Err(e) => unsafe { reply_err(req, e) },
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_mkdir(
    req: fuse::fuse_req_t,
    parent: fuse::fuse_ino_t,
    name: *const c_char,
    mode: libc::mode_t,
) {
    if name.is_null() {
        unsafe { reply_err(req, ENOENT) };
        return;
    }
    let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().mknod(parent as u64, &name, Itype::Dir, mode as u32) {
        Ok(inode) => {
            let mut e = inode_to_entry(&inode);
            unsafe { fuse::fuse_reply_entry(req, &mut e) };
        }
        Err(e) => unsafe { reply_err(req, e) },
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_unlink(req: fuse::fuse_req_t, parent: fuse::fuse_ino_t, name: *const c_char) {
    if name.is_null() {
        unsafe { reply_err(req, ENOENT) };
        return;
    }
    let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().unlink(parent as u64, &name) {
        Ok(inode) => {
            if inode.kind == Itype::File && inode.links == 0 {
                FileStore::unlink(inode.id);
            }
            unsafe { reply_err(req, 0) };
        }
        Err(e) => unsafe { reply_err(req, e) },
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_rmdir(req: fuse::fuse_req_t, parent: fuse::fuse_ino_t, name: *const c_char) {
    if name.is_null() {
        unsafe { reply_err(req, ENOENT) };
        return;
    }
    let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().unlink(parent as u64, &name) {
        Ok(_) => unsafe { reply_err(req, 0) },
        Err(e) => unsafe { reply_err(req, e) },
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_symlink(
    req: fuse::fuse_req_t,
    link: *const c_char,
    parent: fuse::fuse_ino_t,
    name: *const c_char,
) {
    if link.is_null() || name.is_null() {
        unsafe { reply_err(req, ENOENT) };
        return;
    }
    let target = unsafe { CStr::from_ptr(link) }.to_string_lossy().to_string();
    let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().mknod(parent as u64, &name, Itype::Symlink, 0o777) {
        Ok(inode) => {
            let ino = inode.id;
            if let Some(h) = fs.new_file_handle(ino) {
                let mut f = h.lock().unwrap();
                let n = f.write(0, target.as_bytes());
                if n > 0 {
                    let _ = fs.meta().update_inode_after_write(ino, n as u64);
                }
                f.flush(false);
            }
            let mut e = inode_to_entry(&inode);
            unsafe { fuse::fuse_reply_entry(req, &mut e) };
        }
        Err(e) => unsafe { reply_err(req, e) },
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_readlink(req: fuse::fuse_req_t, ino: fuse::fuse_ino_t) {
    let fs = unsafe { fs_from_req(req) };
    if let Some(inode) = fs.meta().load_inode(ino as u64) {
        let len = inode.length as usize;
        if let Some(h) = fs.new_file_handle(ino as u64) {
            let mut f = h.lock().unwrap();
            if let Some(data) = f.read(0, len) {
                if let Ok(s) = CString::new(data) {
                    unsafe {
                        fuse::fuse_reply_readlink(req, s.as_ptr());
                    }
                    return;
                }
            }
        }
    }
    unsafe { reply_err(req, EFAULT) };
}

#[no_mangle]
pub extern "C" fn junkfs_ll_rename(
    req: fuse::fuse_req_t,
    parent: fuse::fuse_ino_t,
    name: *const c_char,
    newparent: fuse::fuse_ino_t,
    newname: *const c_char,
    _flags: u32,
) {
    if name.is_null() || newname.is_null() {
        unsafe { reply_err(req, ENOENT) };
        return;
    }
    let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
    let newname = unsafe { CStr::from_ptr(newname) }.to_string_lossy();
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().rename(parent as u64, &name, newparent as u64, &newname) {
        Ok(_) => unsafe { reply_err(req, 0) },
        Err(e) => unsafe { reply_err(req, e) },
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_link(
    req: fuse::fuse_req_t,
    ino: fuse::fuse_ino_t,
    newparent: fuse::fuse_ino_t,
    newname: *const c_char,
) {
    if newname.is_null() {
        unsafe { reply_err(req, ENOENT) };
        return;
    }
    let newname = unsafe { CStr::from_ptr(newname) }.to_string_lossy();
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().link(ino as u64, newparent as u64, &newname) {
        Ok(inode) => {
            let mut e = inode_to_entry(&inode);
            unsafe { fuse::fuse_reply_entry(req, &mut e) };
        }
        Err(e) => unsafe { reply_err(req, e) },
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_open(req: fuse::fuse_req_t, ino: fuse::fuse_ino_t, fi: *mut fuse::fuse_file_info) {
    let fs = unsafe { fs_from_req(req) };
    if let Some(h) = fs.new_file_handle(ino as u64) {
        let fh = h.lock().unwrap().fh;
        unsafe {
            if !fi.is_null() {
                (*fi).fh = fh;
            }
            fuse::fuse_reply_open(req, fi);
        }
    } else {
        unsafe { reply_err(req, EFAULT) };
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_read(
    req: fuse::fuse_req_t,
    _ino: fuse::fuse_ino_t,
    size: usize,
    off: libc::off_t,
    fi: *mut fuse::fuse_file_info,
) {
    if fi.is_null() {
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    let fs = unsafe { fs_from_req(req) };
    let fh = unsafe { (*fi).fh } as u64;
    if let Some(h) = fs.find_file_handle(fh) {
        let mut f = h.lock().unwrap();
        let inode = match fs.meta().load_inode(f.ino) {
            Some(i) => i,
            None => {
                unsafe { reply_err(req, ENOENT) };
                return;
            }
        };
        if off as u64 >= inode.length {
            unsafe {
                fuse::fuse_reply_buf(req, ptr::null(), 0);
            }
            return;
        }
        let read_size = std::cmp::min(size as u64, inode.length - off as u64) as usize;
        if let Some(data) = f.read(off as u64, read_size) {
            unsafe {
                fuse::fuse_reply_buf(req, data.as_ptr() as *const c_char, data.len());
            }
        } else {
            unsafe { reply_err(req, EFAULT) };
        }
    } else {
        unsafe { reply_err(req, ENOENT) };
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_write(
    req: fuse::fuse_req_t,
    ino: fuse::fuse_ino_t,
    buf: *const c_char,
    size: usize,
    off: libc::off_t,
    fi: *mut fuse::fuse_file_info,
) {
    if fi.is_null() || buf.is_null() {
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    let fs = unsafe { fs_from_req(req) };
    let fh = unsafe { (*fi).fh } as u64;
    if let Some(h) = fs.find_file_handle(fh) {
        let data = unsafe { std::slice::from_raw_parts(buf as *const u8, size) };
        let mut total = 0usize;
        let mut retries = 0u32;
        while total < data.len() {
            let n = {
                let mut f = h.lock().unwrap();
                f.write(off as u64 + total as u64, &data[total..])
            };
            if n == 0 {
                retries += 1;
                if retries > 5 {
                    unsafe { reply_err(req, EIO) };
                    return;
                }
                if !fs.flush_all_caches() {
                    unsafe { reply_err(req, EIO) };
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            total += n;
            retries = 0;
        }
        if total > 0 {
            let _ = fs.meta().update_inode_after_write(ino as u64, off as u64 + total as u64);
        }
        unsafe { fuse::fuse_reply_write(req, total as usize) };
    } else {
        unsafe { reply_err(req, ENOENT) };
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_flush(req: fuse::fuse_req_t, _ino: fuse::fuse_ino_t, _fi: *mut fuse::fuse_file_info) {
    unsafe { reply_err(req, 0) };
}

#[no_mangle]
pub extern "C" fn junkfs_ll_release(req: fuse::fuse_req_t, _ino: fuse::fuse_ino_t, fi: *mut fuse::fuse_file_info) {
    if fi.is_null() {
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    let fs = unsafe { fs_from_req(req) };
    let fh = unsafe { (*fi).fh } as u64;
    fs.remove_file_handle(fh);
    unsafe { reply_err(req, 0) };
}

#[no_mangle]
pub extern "C" fn junkfs_ll_opendir(req: fuse::fuse_req_t, ino: fuse::fuse_ino_t, fi: *mut fuse::fuse_file_info) {
    if fi.is_null() {
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    let fs = unsafe { fs_from_req(req) };
    if let Some(h) = fs.new_dir_handle_alloc() {
        let fh = h.lock().unwrap().fh;
        fs.meta().load_dentry(ino as u64, &mut h.lock().unwrap());
        unsafe {
            (*fi).fh = fh;
            fuse::fuse_reply_open(req, fi);
        }
    } else {
        unsafe { reply_err(req, EFAULT) };
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_readdir(
    req: fuse::fuse_req_t,
    _ino: fuse::fuse_ino_t,
    size: usize,
    off: libc::off_t,
    fi: *mut fuse::fuse_file_info,
) {
    if fi.is_null() {
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    let fs = unsafe { fs_from_req(req) };
    let fh = unsafe { (*fi).fh } as u64;
    let Some(h) = fs.find_dir_handle(fh) else {
        unsafe { reply_err(req, ENOENT) };
        return;
    };
    let handle = h.lock().unwrap();
    let mut buf = vec![0u8; size];
    let mut used = 0usize;
    let mut off_idx = off as usize;
    while let Some(entry) = handle.get_at(off_idx) {
        let name = match CString::new(entry.name.as_str()) {
            Ok(s) => s,
            Err(_) => {
                unsafe { reply_err(req, EFAULT) };
                return;
            }
        };
        let mut st: fuse::stat = unsafe { std::mem::zeroed() };
        st.st_ino = entry.ino as fuse::fuse_ino_t;
        st.st_mode = kind_to_mode(entry.kind) as libc::mode_t;
        let next_off = (off_idx + 1) as libc::off_t;
        let ent = unsafe {
            fuse::fuse_add_direntry(
                req,
                buf.as_mut_ptr().add(used) as *mut c_char,
                (size - used) as usize,
                name.as_ptr(),
                &st,
                next_off,
            )
        } as usize;
        if ent > size - used {
            break;
        }
        used += ent;
        off_idx += 1;
    }
    unsafe {
        fuse::fuse_reply_buf(req, buf.as_ptr() as *const c_char, used);
    }
}

#[no_mangle]
pub extern "C" fn junkfs_ll_releasedir(req: fuse::fuse_req_t, _ino: fuse::fuse_ino_t, fi: *mut fuse::fuse_file_info) {
    if fi.is_null() {
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    let fs = unsafe { fs_from_req(req) };
    let fh = unsafe { (*fi).fh } as u64;
    fs.remove_dir_handle(fh);
    unsafe { reply_err(req, 0) };
}

#[no_mangle]
pub extern "C" fn junkfs_ll_fsync(
    req: fuse::fuse_req_t,
    ino: fuse::fuse_ino_t,
    datasync: i32,
    fi: *mut fuse::fuse_file_info,
) {
    let fs = unsafe { fs_from_req(req) };
    if let Some(h) = if fi.is_null() { None } else { fs.find_file_handle(unsafe { (*fi).fh } as u64) } {
        h.lock().unwrap().flush(false);
    }
    let datasync = datasync != 0;
    if let Err(e) = FileStore::fsync(ino as u64, datasync) {
        log::error!("can't fsync ino {} error {}", ino, e);
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    if datasync {
        if let Err(e) = fs.meta().flush_inode(ino as u64) {
            log::error!("can't sync inode {} error {}", ino, e);
            unsafe { reply_err(req, EFAULT) };
            return;
        }
        if let Err(e) = fs.meta().commit_pending() {
            log::error!("can't commit metadata error {}", e);
            unsafe { reply_err(req, EFAULT) };
            return;
        }
    } else if let Err(e) = fs.meta().sync() {
        log::error!("can't sync metadata for ino {} error {}", ino, e);
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    unsafe { reply_err(req, 0) };
}

#[no_mangle]
pub extern "C" fn junkfs_ll_fsyncdir(
    req: fuse::fuse_req_t,
    ino: fuse::fuse_ino_t,
    _datasync: i32,
    _fi: *mut fuse::fuse_file_info,
) {
    let fs = unsafe { fs_from_req(req) };
    if let Err(e) = fs.meta().sync() {
        log::error!("can't fsyncdir ino {} error {}", ino, e);
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    unsafe { reply_err(req, 0) };
}

#[no_mangle]
pub extern "C" fn junkfs_ll_create(
    req: fuse::fuse_req_t,
    parent: fuse::fuse_ino_t,
    name: *const c_char,
    mode: libc::mode_t,
    fi: *mut fuse::fuse_file_info,
) {
    if name.is_null() || fi.is_null() {
        unsafe { reply_err(req, EFAULT) };
        return;
    }
    let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
    let fs = unsafe { fs_from_req(req) };
    match fs.meta().mknod(parent as u64, &name, Itype::File, mode as u32) {
        Err(e) => unsafe { reply_err(req, e) },
        Ok(inode) => {
            let ino = inode.id;
            if let Some(handle) = fs.new_file_handle(ino) {
                let fh = handle.lock().unwrap().fh;
                unsafe {
                    (*fi).fh = fh;
                    let mut e = inode_to_entry(&inode);
                    fuse::fuse_reply_create(req, &mut e, fi);
                }
            } else {
                unsafe { reply_err(req, EFAULT) };
            }
        }
    }
}
