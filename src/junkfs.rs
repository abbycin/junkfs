use junkfs::fs::Fs;
use junkfs::logger::Logger;
use libc::{sighandler_t, SIGINT, SIGTERM};

fn main() {
    let log_path = "/tmp/junkfs.log";
    Logger::init().add_file(&log_path, true);
    log::set_max_level(log::LevelFilter::Warn);
    if std::env::args().len() != 3 {
        eprintln!("{} meta_path mount_point", std::env::args().nth(0).unwrap());
        std::process::exit(1);
    }

    println!("log write to {}", log_path);
    let meta_path = std::env::args().nth(1).unwrap();
    let mount_point = std::env::args().nth(2).unwrap();

    setup_signal_handler();

    let junkfs = Fs::new(meta_path);
    match junkfs {
        Err(e) => {
            log::error!("load filesystem fail, error {e}");
            std::process::exit(1);
        }
        Ok(junkfs) => {
            let options = [
                fuser::MountOption::FSName("jfs".to_string()),
                fuser::MountOption::Subtype("jfs".to_string()),
            ];
            // let session = fuser::spawn_mount2(junkfs, &mount_point, &options).expect("can't mount");
            // wait_signal();
            // session.join();

            let r = fuser::mount2(junkfs, &mount_point, &options);
            match r {
                Err(e) => {
                    log::error!("mount fail, error {}", e.to_string());
                    std::process::exit(1);
                }
                Ok(()) => {}
            }
        }
    }
}

static mut IS_QUIT: bool = false;

extern "C" fn handle_signal(_sig: i32) {
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

#[allow(dead_code)]
fn wait_signal() {
    unsafe {
        libc::pause();
    }
}
