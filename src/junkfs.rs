use junkfs::fs::Fs;
use junkfs::logger::Logger;
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let level = std::env::var("JUNK_LEVEL").unwrap_or("INFO".to_string());
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

    let junkfs = Fs::new(meta_path);
    match junkfs {
        Err(e) => {
            log::error!("load filesystem fail, error {e}");
            std::process::exit(1);
        }
        Ok(junkfs) => {
            let options = vec![
                fuser::MountOption::FSName("jfs".to_string()),
                fuser::MountOption::Subtype("jfs".to_string()),
                // fuser::MountOption::AutoUnmount,
            ];
            println!("Starting FUSE mount at {:?}...", mount_point);
            if let Err(e) = fuser::mount2(junkfs, &mount_point, &options) {
                log::error!("FUSE mount failed: {:?}", e);
                eprintln!("FUSE mount failed: {:?}", e);
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
