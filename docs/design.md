This is an experimental filesystem implementation in Rust + FUSE. The current version is based on the `libfuse3` low-level C API (not fuser), uses a multithreaded session loop, stores metadata in `mace`, and stores file data in regular files under a local sharded directory.

## 1. Overall architecture

The core path of `junkfs` is:

1. Linux VFS enters `junkfs_ll_*` via FUSE requests
2. `Fs` manages file/dir handles, inode reference counts, and writeback-thread scheduling
3. `Meta` commits inode/dentry/superblock/imap transactions
4. `FileStore` performs the actual data-file I/O (`pwritev/pwrite`)

Writes follow the typical writeback pattern: data lands in the cache first, then a background thread flushes it to disk in batches and commits the metadata.

## 2. Metadata design

Metadata is stored as key-value entries in a `mace` bucket. The core objects are:

- `SuperBlock`
- `Inode`
- `Dentry`
- `imap` (grouped inode bitmaps)

### 2.1 SuperBlock

`superblock` only keeps global base information, not the inode table/data map:

```rust
struct SuperBlock {
    ino: Ino,
    uri: String,
    version: u32,
    total_inodes: u64,
    group_size: u64,
    group_count: u64,
}
```

Currently `version = 3`. `uri` is the root path of the data files.

### 2.2 Inode / Dentry

- inode key: `i_$ino`
- dentry key: `d_$parent_$name`

`Itype` currently supports:

- `File`
- `Dir`
- `Symlink`

### 2.3 Inode allocation bitmap (imap)

Inode allocation uses a two-level bitmap:

- `imap_sum`: 1 bit per group, indicating whether the group still has free inodes
- `imap_$gid`: in-group bitset marking individual inode occupancy

Groups are loaded on demand during allocation/free, avoiding full-bitmap reads and writes.

### 2.4 Pending commit model

Metadata changes are first written to an in-memory `pending`:

- `puts: HashMap<String, Vec<u8>>`
- `dels: HashSet<String>`

A background thread triggers `commit_pending()` to batch-commit transactions by threshold/time. Commits use a "take-the-batch-then-commit" approach to avoid repeatedly cloning large values on the retry path, keeping peak memory down.

### 2.5 Delayed deletion of open files

To match Linux semantics, `unlink`/`rename`-over-target defers reclamation of "still-open regular files":

1. Remove the directory entry first (invisible to users)
2. Once the inode link count reaches 0, add it to `orphan_inodes`
3. Run `finalize_unlink` when the last file handle is released
4. Finally delete the data file

This guarantees that "an already-open fd stays usable after unlink".

## 3. Data design

### 3.1 Storage layout

Each inode maps to one data file at:

```text
$store_path/<shard1>/<shard2>/<ino>
```

The two-level directory sharding bounds the entry count per directory. Data files are sparse files; logical offsets map directly to physical offsets.

### 3.2 Data caching and flush

`CacheStore` does writeback using a page cache (from `MemPool`):

- default mempool: `256MB`
- dirty-data flush threshold: `64MB`
- timeout flush: `200ms`
- large-write fast path: writes that are "aligned and large enough" take the direct-write path, bypassing the page cache

The background writeback thread scans the cache every `100ms` to flush to disk and drives metadata commits.

### 3.3 Page-cache control after file writes

After completing a data write, `FileStore` calls `posix_fadvise(..., POSIX_FADV_DONTNEED)` to try dropping the just-written data pages, reducing kernel cache amplification under FUSE.

## 4. FUSE integration

Current integration:

- `libfuse3` low-level C API
- `fuse_session_loop_mt` multithreaded loop
- `max_write/max_read/max_readahead = 16MB`
- `async_read` enabled
- `writeback_cache` enabled by default (disable with `JUNK_DISABLE_WBC=1`)
- negative-entry caching on lookup miss (short TTL)

## 5. Consistency semantics

### 5.1 fsync semantics

- `fsync(datasync=true)`:
  - flush the file-handle cache
  - `FileStore::fsync(datasync=true)`
  - `flush_inode(ino)` + `commit_pending()`
- `fsync(datasync=false)`:
  - flush the file-handle cache
  - `FileStore::fsync(datasync=false)`
  - `meta.sync()`
- `fsyncdir`: goes through `meta.sync()`

### 5.2 Crash model

The default is performance-first writeback: persistence is not guaranteed when each syscall returns. Stronger semantics rely on `fsync/fsyncdir`.

## 6. Memory budget

The current default memory ceiling comes from three parts:

1. `MemPool`: `256MB`
2. `mace` metadata cache:
   - `cache_capacity = 256MB`
   - `cache_count/stat_mask_cache_count = 4096`
   - `data/blob handle cache = 64`
3. In-process runtime objects and indexes (handle maps, dentry/index caches, pending, etc.)

The overall goal is to keep process memory in a controlled range while leaving more transient usage to the Linux page cache.

## 7. Observability and debugging

- `JUNK_LEVEL`: log level (default `ERROR`)
- `JUNK_DISABLE_WBC`: disable the FUSE writeback cache
- `JUNK_ENABLE_INO_REUSE`: whether inode reuse is enabled (on by default)
- `JUNK_STRICT_INVARIANT`: enable strict consistency assertions (off by default)
- `JUNK_VERIFY_FLUSH`: post-write verification (debug only, off by default)
- `stats` feature: emit write/flush statistics

## 8. Known boundaries

- This is a testing/experimental filesystem; full POSIX compatibility is not a goal
- Metadata depends on `mace`; no multi-backend abstraction is planned
- Crash consistency under writeback is weaker than that of strictly synchronous filesystems
