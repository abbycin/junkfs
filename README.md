# junkfs

[中文](./README_en.md)

An experimental Rust filesystem based on the **libfuse3 low-level C API**.
It started as a FUSE practice project and now mainly serves as the test vehicle for the `mace` metadata engine.

Detailed design: `docs/design.md`

## Features

- FUSE multithreaded session loop (`fuse_session_loop_mt`)
- Metadata storage: `mace-kv`
- Data storage: one data file per inode, sharded across a two-level directory layout
- Writeback data path + background writeback thread
- Delayed deletion of open files (closer to Linux semantics)

## Dependencies

- Linux
- `fuse3` runtime and development headers (e.g. `fuse3`, `fuse3-devel` / `libfuse3-dev`)
- An accessible mount-point directory

## Implemented operations

- `lookup`
- `getattr` / `setattr`
- `create` / `mknod` / `open` / `release`
- `read` / `write` / `flush`
- `mkdir` / `opendir` / `readdir` / `releasedir`
- `unlink` / `rmdir` / `rename`
- `link` / `symlink` / `readlink`
- `fsync` / `fsyncdir`

> Note: this is an experimental filesystem; full POSIX compatibility is not a goal.

## Quick start

### 1) Format

`mkfs` wipes and recreates `meta_path` and `store_path`.

```bash
cargo run --bin mkfs --release -- /nvme/meta /nvme/store
```

### 2) Mount

```bash
mkdir -p ~/jfs
cargo run --bin junkfs --release -- /nvme/meta ~/jfs
```

Logs go to `/tmp/junkfs.log` by default.

### 3) Use

In another terminal, operate on `~/jfs` normally, e.g.:

```bash
tar xf /home/neo/Downloads/linux-6.12.69.tar.xz -C ~/jfs
cd ~/jfs/linux-6.12.69
make alldefconfig
make -j4
```

### 4) Unmount

```bash
umount ~/jfs
```

## Environment variables

- `JUNK_LEVEL`: log level, default `ERROR`
- `JUNK_DISABLE_WBC=1`: disable the FUSE writeback cache (enabled by default)
- `JUNK_ENABLE_INO_REUSE=0|1`: control inode reuse (default `1`)
- `JUNK_STRICT_INVARIANT=1`: enable strict consistency assertions (disabled by default)
- `JUNK_VERIFY_FLUSH=1`: enable post-write verification (debug only, disabled by default)

## stats (optional)

Write-statistics logging can be enabled via a feature:

```bash
cargo run --bin junkfs --release --features stats -- /nvme/meta ~/jfs
```

## Known limitations

- The default writeback policy favors performance; crash consistency relies on `fsync/fsyncdir`
- The metadata backend is fixed to `mace-kv`
- Built for testing and experimentation; not recommended for direct use as a production filesystem
