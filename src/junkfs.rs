use junkfs::fs::Fs;
use junkfs::logger::Logger;
use junkfs_fuse as fuse;
use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::str::FromStr;

fn main() {
    let level = std::env::var("JUNK_LEVEL").unwrap_or("ERROR".to_string());
    let log_path = "/tmp/junkfs.log";
    Logger::init().add_file(log_path, true);
    log::set_max_level(log::LevelFilter::from_str(&level).unwrap());

    if std::env::args().len() != 3 {
        eprintln!("{} meta_path mount_point", std::env::args().next().unwrap());
        std::process::exit(1);
    }

    let meta_path = std::env::args().nth(1).unwrap();
    let mount_point = std::env::args().nth(2).unwrap();
    println!(
        "log write to {} level {} meta_path {:?} mount_point {:?}",
        log_path, level, meta_path, mount_point
    );

    let fs = match Fs::new(meta_path) {
        Ok(fs) => fs,
        Err(e) => {
            log::error!("load filesystem fail, error {e}");
            std::process::exit(1);
        }
    };

    let mut fs = Box::new(fs);
    let mut args = fuse::fuse_args {
        argc: 0,
        argv: ptr::null_mut(),
        allocated: 0,
    };

    let prog = CString::new("junkfs").unwrap();
    let opt_flag = CString::new("-o").unwrap();
    let opt_val = CString::new("fsname=jfs,subtype=jfs,max_read=16777216").unwrap();
    unsafe {
        fuse::fuse_opt_add_arg(&mut args, prog.as_ptr());
        fuse::fuse_opt_add_arg(&mut args, opt_flag.as_ptr());
        fuse::fuse_opt_add_arg(&mut args, opt_val.as_ptr());
    }

    let fs_ptr = fs.as_mut() as *mut Fs as *mut c_void;
    let se = unsafe { fuse::junkfs_fuse_session_new(&mut args, fs_ptr) };
    if se.is_null() {
        eprintln!("fuse_session_new failed");
        unsafe { fuse::fuse_opt_free_args(&mut args) };
        fs.shutdown();
        std::process::exit(1);
    }

    let mnt = CString::new(mount_point).unwrap();
    let mnt_res = unsafe { fuse::fuse_session_mount(se, mnt.as_ptr()) };
    if mnt_res != 0 {
        unsafe {
            fuse::fuse_session_destroy(se);
            fuse::fuse_opt_free_args(&mut args);
        }
        eprintln!("fuse_session_mount failed");
        fs.shutdown();
        std::process::exit(1);
    }

    let _ = unsafe { fuse::fuse_set_signal_handlers(se) };
    let mut cfg = fuse::fuse_loop_config {
        clone_fd: 1,
        max_idle_threads: 16,
    };
    let loop_res = unsafe { fuse::junkfs_fuse_session_loop_mt(se, &mut cfg) };
    unsafe {
        fuse::fuse_session_unmount(se);
        fuse::fuse_remove_signal_handlers(se);
        fuse::fuse_session_destroy(se);
        fuse::fuse_opt_free_args(&mut args);
    }

    if loop_res != 0 {
        log::error!("fuse loop exit with {}", loop_res);
    }

    fs.shutdown();
    drop(fs);
}
