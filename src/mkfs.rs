use junkfs::meta::Meta;

fn main() {
    if std::env::args().len() != 3 {
        eprintln!("{} meta_path store_path", std::env::args().next().unwrap());
        std::process::exit(1);
    }

    let meta_path = std::env::args().nth(1).unwrap();
    let mut store_path = std::env::args().nth(2).unwrap();

    while store_path.ends_with('/') {
        store_path.remove(store_path.len() - 1);
    }

    let r = Meta::format(&meta_path, &store_path);

    match r {
        Err(e) => {
            eprintln!("can't format, error {}", e);
            std::process::exit(1);
        }
        Ok(()) => {
            println!("formated meta_path => {} store_path => {}", meta_path, store_path);
        }
    }
}
