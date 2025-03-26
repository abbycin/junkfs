use junkfs::fs::Fs;
use junkfs::logger::Logger;
use std::str::FromStr;
use tokio::signal::unix::{signal, SignalKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let level = std::env::var("JUNK_LEVEL").unwrap_or("WARN".to_string());
    let log_path = "/tmp/junkfs.log";
    Logger::init().add_file(log_path, true);
    log::set_max_level(log::LevelFilter::from_str(&level).unwrap());
    if std::env::args().len() != 3 {
        eprintln!("{} meta_path mount_point", std::env::args().next().unwrap());
        std::process::exit(1);
    }

    println!("log write to {} level {}", log_path, level);
    let meta_path = std::env::args().nth(1).unwrap();
    let mount_point = std::env::args().nth(2).unwrap();

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
            let session = fuser::spawn_mount2(junkfs, &mount_point, &options).expect("can't mount");
            let mut sig_int = signal(SignalKind::interrupt())?;
            let mut sig_term = signal(SignalKind::terminate())?;

            tokio::select! {
                _ = sig_int.recv() => session.join(),
                _ = sig_term.recv() => session.join(),
            }
        }
    }
    Ok(())
}
