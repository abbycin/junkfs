这是一个 Rust + FUSE 的实验型文件系统实现。当前版本基于 `libfuse3` 低层 C API（非 fuser），采用多线程会话循环，元数据存放在 `mace`，文件数据存放在本地分片目录中的普通文件。

## 1. 总体架构

`junkfs` 的核心路径如下：

1. Linux VFS 通过 FUSE 请求进入 `junkfs_ll_*`
2. `Fs` 负责文件句柄/目录句柄、inode 引用计数、写回线程调度
3. `Meta` 负责 inode/dentry/superblock/imap 的事务提交
4. `FileStore` 负责实际数据文件读写（`pwritev/pwrite`）

写入是典型 writeback 模式：先入缓存，再由后台线程批量刷盘与提交元数据。

## 2. 元数据设计

元数据按 key-value 形式存放在 `mace` bucket 中，核心对象为：

- `SuperBlock`
- `Inode`
- `Dentry`
- `imap`（分组 inode 位图）

### 2.1 SuperBlock

`superblock` 只保存全局基础信息，不保存 inode table/data map：

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

当前 `version = 3`。`uri` 是数据文件根目录路径。

### 2.2 Inode / Dentry

- inode key: `i_$ino`
- dentry key: `d_$parent_$name`

`Itype` 当前支持：

- `File`
- `Dir`
- `Symlink`

### 2.3 inode 分配位图（imap）

inode 分配采用两层位图：

- `imap_sum`：每个组 1 bit，表示该组是否仍有空闲 inode
- `imap_$gid`：组内 bitset，表示具体 inode 占用

分配/释放时按需加载 group，避免全量位图读写。

### 2.4 pending 提交模型

元数据变更先写入内存 `pending`：

- `puts: HashMap<String, Vec<u8>>`
- `dels: HashSet<String>`

后台线程按阈值/时间触发 `commit_pending()` 批量提交事务。提交过程采用“取走批次再提交”的方式，避免大 value 在重试路径上反复 clone，降低峰值内存。

### 2.5 open 文件的延迟删除

为对齐 Linux 语义，`unlink/rename` 覆盖目标时对“仍被打开的普通文件”采用延迟回收：

1. 先删除目录项（对用户不可见）
2. inode 链接数到 0 后加入 `orphan_inodes`
3. 最后一个 file handle release 时执行 `finalize_unlink`
4. 最终删除数据文件

这样可以保证“已打开 fd 在 unlink 后仍可继续访问”。

## 3. 数据设计

### 3.1 存储布局

每个 inode 对应一个数据文件，路径为：

```text
$store_path/<shard1>/<shard2>/<ino>
```

通过两级目录分片控制单目录项数量。数据文件是稀疏文件，逻辑偏移直接映射到物理偏移。

### 3.2 数据缓存与刷写

`CacheStore` 使用页缓存（来自 `MemPool`）进行 writeback：

- 默认 mempool: `256MB`
- 脏数据阈值刷写: `64MB`
- 超时刷写: `200ms`
- 大写优化：当写请求满足“对齐且足够大”时走 direct write 路径，绕过页缓存

后台写回线程每 `100ms` 扫描缓存并刷盘，同时推动元数据提交。

### 3.3 文件写入后的页缓存控制

`FileStore` 在完成数据写入后会调用 `posix_fadvise(..., POSIX_FADV_DONTNEED)` 尝试丢弃刚写入的数据页缓存，减少 FUSE 场景下内核缓存放大。

## 4. FUSE 接入

当前接入方式：

- `libfuse3` low-level C API
- `fuse_session_loop_mt` 多线程循环
- `max_write/max_read/max_readahead = 16MB`
- 启用 `async_read`
- 默认启用 `writeback_cache`（可用 `JUNK_DISABLE_WBC=1` 关闭）
- lookup miss 使用 negative entry 缓存（短 TTL）

## 5. 一致性语义

### 5.1 fsync 语义

- `fsync(datasync=true)`：
  - 刷 file handle 缓存
  - `FileStore::fsync(datasync=true)`
  - `flush_inode(ino)` + `commit_pending()`
- `fsync(datasync=false)`：
  - 刷 file handle 缓存
  - `FileStore::fsync(datasync=false)`
  - `meta.sync()`
- `fsyncdir`：走 `meta.sync()`

### 5.2 崩溃模型

默认是性能优先的 writeback：不保证每次系统调用返回后都已持久化。需要更强语义时依赖 `fsync/fsyncdir`。

## 6. 内存占用策略

当前默认内存上限主要来自三部分：

1. `MemPool`: `256MB`
2. `mace` 元数据缓存:
   - `cache_capacity = 256MB`
   - `cache_count/stat_mask_cache_count = 4096`
   - `data/blob handle cache = 64`
3. 进程运行时对象与索引（handle map、dentry/index 缓存、pending 等）

整体目标是将进程内存保持在可控区间，同时把更多瞬时占用留给 Linux 页缓存。

## 7. 可观测性与调试

- `JUNK_LEVEL`：日志级别（默认 `ERROR`）
- `JUNK_DISABLE_WBC`：关闭 FUSE writeback cache
- `JUNK_ENABLE_INO_REUSE`：是否开启 inode 复用（默认开）
- `JUNK_STRICT_INVARIANT`：开启严格一致性断言（默认关）
- `JUNK_VERIFY_FLUSH`：写后校验（调试用，默认关）
- `stats` feature：输出写入/flush 统计

## 8. 当前已知边界

- 这是测试/实验型文件系统，不追求完整 POSIX 兼容
- 元数据依赖 `mace`，不再设计多后端抽象
- writeback 模式下崩溃一致性弱于严格同步文件系统
