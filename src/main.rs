use chaosfs::fs::SuperBlock;
use fuser::{self, Filesystem};
use serde::Serialize;
use sled;

struct MyFS {
    name: String,
}

impl Filesystem for MyFS {
    fn init(
        &mut self,
        req: &fuser::Request<'_>,
        _cfg: &mut fuser::KernelConfig,
    ) -> Result<(), i32> {
        println!(
            "unique {}, uid {}, gid {}, pid {}",
            req.unique(),
            req.uid(),
            req.gid(),
            req.pid()
        );
        Ok(())
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let hello = "hello world";
        reply.data(hello.as_bytes())
    }
}

fn main() {
    if std::env::args().len() != 2 {
        eprintln!("{} mount_point", std::env::args().nth(0).unwrap());
        std::process::exit(1);
    }

    let db = sled::open("/tmp/fs").expect("can't create/open db");

    db.flush().expect("flush db fail");

    let sb = chaosfs::fs::SuperBlock::new("chaosfs".to_string(), 233, 666);
    let s11n = bincode::serialize(&sb).expect("can't serialize");

    db.insert("superblock", s11n);

    let de = db.get("superblock").unwrap();

    if de.is_some() {
        let de = bincode::deserialize::<SuperBlock>(&de.unwrap());
        println!("superblock => {:?}", de.unwrap());
    }

    chaosfs::fs::superblock::hello();

    std::process::exit(0);

    let fs = MyFS {
        name: "chaosfs".to_string(),
    };
    let path = std::env::args().nth(1).unwrap();
    let options = [fuser::MountOption::FSName("chaosfs".to_string())];
    fuser::mount2(fs, &path, &options).expect_err("can't mount");
}
