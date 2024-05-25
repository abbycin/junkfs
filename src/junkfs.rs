use junkfs::fs::Fs;
use libc::{sighandler_t, SIGINT, SIGTERM};
use std::thread::sleep;
use std::time::Duration;

fn main() {
    if std::env::args().len() != 3 {
        eprintln!("{} meta_path mount_point", std::env::args().nth(0).unwrap());
        std::process::exit(1);
    }

    setup_signal_handler();

    let meta_path = std::env::args().nth(1).unwrap();
    let mount_point = std::env::args().nth(2).unwrap();

    let junkfs = Fs::new(meta_path);
    match junkfs {
        Err(e) => {
            eprintln!("load filesystem fail, error {e}");
            std::process::exit(1);
        }
        Ok(junkfs) => {
            let options = [fuser::MountOption::FSName("chaosfs".to_string())];
            let session = fuser::spawn_mount2(junkfs, &mount_point, &options).expect("can't mount");
            wait_signal();
            session.join();
        }
    }

    std::process::exit(0);
}

static mut IS_QUIT: bool = false;

extern "C" fn handle_signal(sig: i32) {
    unsafe {
        IS_QUIT = true;
    }
}

fn setup_signal_handler() {
    unsafe {
        let handler = handle_signal as sighandler_t;
        libc::signal(SIGTERM, handler);
        libc::signal(SIGINT, handler);
    }
}

fn wait_signal() {
    unsafe {
        while !IS_QUIT {
            sleep(Duration::from_secs(1));
        }
    }
}
