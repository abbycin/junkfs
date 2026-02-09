## A toy filesystem based on FUSE (It has now become a testing tool for [Mace](https://github.com/abbycin/mace))

supported operations:

- `create`
- `mknod`
- `open`
- `release`
- `unlink`
- `mkdir`
- `opendir`
- `readdir`
- `rmdir`
- `releasedir`
- `read`
- `write`
- `lookup`
- `getattr`
- `setattr`

**NOTE**: This is not fully POSIX compliant, as fully implementing POSIX semantics is tedious and complex

## How to use

This project now uses **libfuse3 low-level C API** with a multi-threaded session loop.

format

```bash
$ cargo run --bin mkfs /tmp/meta /tmp/data                                                                                           0 [12:12:58]
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.02s
     Running `target/debug/mkfs /tmp/meta /tmp/data`
formated meta_path => /tmp/meta store_path => /tmp/data
```

mount to `~/jfs`

```bash
$ mkdir ~/jfs
$ cargo run --bin junkfs /tmp/meta ~/jfs                                                                                             0 [12:13:35]
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.02s
     Running `target/debug/junkfs /tmp/meta /home/abby/jfs`
log write to /tmp/junkfs.log
```

in other terminal

```bash
$ cat > x.c                                                                                                                          0 [12:14:06]
#include <stdio.h>

int main() {
        printf("hello world!\n");
}
$ cc x.c
$ ./a.out                                                                                                                            0 [12:14:50]
hello world!
$ stat a.out                                                                                                                         0 [12:14:52]
  File: a.out
  Size: 19832           Blocks: 1          IO Block: 4096   regular file
Device: 0,42    Inode: 3           Links: 1
Access: (0755/-rwxr-xr-x)  Uid: ( 1000/    abby)   Gid: ( 1000/    abby)
Access: 2024-06-02 12:14:50.000000000 +0800
Modify: 2024-06-02 12:14:50.000000000 +0800
Change: 2024-06-02 12:14:50.000000000 +0800
 Birth: -
$ stat x.c                                                                                                                           0 [12:14:56]
  File: x.c
  Size: 62              Blocks: 1          IO Block: 4096   regular file
Device: 0,42    Inode: 2           Links: 1
Access: (0644/-rw-r--r--)  Uid: ( 1000/    abby)   Gid: ( 1000/    abby)
Access: 2024-06-02 12:14:26.000000000 +0800
Modify: 2024-06-02 12:14:26.000000000 +0800
Change: 2024-06-02 12:14:26.000000000 +0800
 Birth: -
$ ls -li                                                                                                                             0 [12:19:18]
total 1
3 -rwxr-xr-x 1 abby abby 19832 Jun  2 12:14 a.out*
2 -rw-r--r-- 1 abby abby    62 Jun  2 12:14 x.c
```

umount, also notify `junkfs` to quit

```bash
$ umount ~/jfs
```
