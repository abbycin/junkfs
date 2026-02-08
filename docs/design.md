这是一个`Rust`和`FUSE`的练习项目，目前已实现数个常用的`POSIX`文件操作方法，这里介绍`junkfs`的设计

### 元数据设计

在`junkfs`中，元数据序列化后存在`kvdb`中（~~使用`sled`实现~~ 使用 `mace` 实现），因此在格式化时需要提供 `kvdb`的存储路径，同时`junkfs`
旨在做本地的文件系统，因此，在格式化时还需要将数据存储路径写入到元数据中

在`junkfs`中，元数据分为

1. `SuperBlock`
1. `Dentry`
2. `Inode`

#### SuperBlock

由于使用了`kvdb`在`superblock`中可以将`dentry`剥离出来，由于`dentry`是一种特殊的文件，因此也会占有`inode`
，因此在`superblock`中只保留基础参数与数据存储路径，`inode`空闲管理采用**分组位图**，单独存放在`kvdb`中，
以降低写放大与读放大。`superblock`结构如下

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

`inode`分配位图由两个层次构成：

- `imap_sum`：每个 group 一个 bit，表示该组是否还有空闲 inode
- `imap_$gid`：每个 group 内的具体位图，按需加载

这样可以避免每次分配/释放都修改整个大位图。

#### Dentry

和大多数文件系统一样，在`junkfs`中`dentry`也用于存放名字到`inode`的映射，但是在`junkfs`中`dentry`是平坦的存放在`kvdb`
中，一个目录中文件的表示为 `d_$ino_$name`，其中`$ino`是目录的`inode`编号，而`$name`是目录中文件的名字，`dentry`的结构如下

```rust
struct Dentry {
    parent: Ino, // 目录的inode
    ino: Ino,    // 文件自己的inode
    name: String,
}
```

#### Inode

和`dentry`一样，`inode`也是平坦的存放在`kvdb`中，一个文件的表示为`i_$ino`，其中`$ino`是这个文件的`inode`编号，`inode`的结构如下

```rust
struct Inode {
    pub id: Ino,
    pub parent: Ino,
    pub kind: Itype,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub length: u64,
    pub links: u32,
}

pub enum Itype {
    File,
    Dir,
}
```

在`junkfs`中仅支持两种类型的文件：普通文件、目录

### 数据设计

在`junkfs`中，数据按固定块大小进行逻辑划分，但物理存储改为**每个 inode 对应一个数据文件**，以避免大量目录与小文件导致的元数据膨胀。文件存储路径采用两级分片目录：

```
$store_path/<shard1>/<shard2>/<ino>
```

其中`shard1`和`shard2`来自`ino`的低位切分，用于控制单目录的目录项数量。数据文件是**稀疏文件**，逻辑块通过固定偏移映射到数据文件：

```
offset = block_id * FS_BLK_SIZE + block_off
```

这样既保留块语义（用于缓存与 I/O 拆分），又避免为每个块创建文件。`truncate` 会调用 `set_len` 更新数据文件长度，扩展部分读零，缩小后不会读到旧数据。

NOTE: 最好使用 XFS 这类动态分配 inode 的文件系统做数据存储

#### 数据一致性与 fsync 语义

`junkfs`采用**写回(writeback)**策略：数据与元数据会先进入内存缓存，由后台线程周期性刷盘并批量提交元数据。
因此在**没有显式 fsync** 的情况下，崩溃一致性弱于传统的严格同步方案，但性能更高。

`fsync` 语义区分如下：

- `fsync(datasync=true)`：先刷数据缓存，再 `sync_data`，随后写回该 inode 元数据并提交 pending 事务
- `fsync(datasync=false)`：先刷数据缓存，再 `sync_all`，随后 `meta.sync()`（写回 inode/imap/sb 并提交 pending）
- `fsyncdir`：仅同步元数据（`meta.sync()`）

为了降低开销，**数据文件创建/删除不再对父目录 `fsync`**，这一点偏向性能而非强一致语义。

#### 文件抽象

在`junkfs`中也有类似于内核中的`struct file`结构，这个结构就是`FileHandle`，它包含一个全局唯一的`id`
，在文件打开时分配，关闭时释放，一个文件可以打开多次，因此存在一个`inode`对应多个`FileHandle`的情况

在`FileHandle`中实现了`read`、`write`和`flush`功能，这样是对照`struct file`设计的，`Inode`负责结构管理，`FileHandle`负责内容管理

#### 目录抽象

在`junkfs`中目录和文件是独立的实现，但它们都实现了一个比较的`trait`，这样它们就可以存放在同一个以`ino`
为`key`，`Box<dyn Trait>`为值的表中，统一资源管理。同样，和`FileHandle`一样，目录结构`DirHandle`中也有一个全局唯一的`id`
，这个结构的作用是：在列出目录内容时保证原子性。即在打开目录时，将目录内容读取出来，在列出内容时向外吐出读取的目录项，这样可以避免当目录在读取同时又有新建或删除操作在目录下进行，导致列出的目录项出现重复或者丢失乱序等问题（这也是`POSIX`
设计的问题）

### 缓存

在`junkfs`中有两类缓存：1. 元数据缓存，2. 数据缓存

##### 元数据缓存

当前实现包含轻量级元数据缓存与索引：

- `inode`缓存：读缓存 + dirty 标记，写回由后台线程或 `fsync` 触发
- `dentry` LRU：缓存目录项存在性与 inode 号，减少 `kvdb` 读取
- `dir_index`：按目录维护 `name -> ino` 的哈希索引，首次 `readdir/lookup` 时从 `kvdb` 扫描构建，并与 pending 变更合并

这套缓存不会改变持久化语义，只是减少查询路径上的随机读。

##### 数据缓存

数据缓存使用固定大小的页面组成，在设计上，当缓存不足时将缓存刷到文件中，或在刷写超时后刷到文件。当前实现不再要求在 `close/read` 前强制刷盘，而是由后台线程周期性写回。
当前实现已引入**后台写回线程**，以实现周期性 flush：

- 数据写入先进入 `CacheStore`（`MemPool` 页缓存），写回条件：缓存超过阈值或超时
- 后台线程定期 flush 缓存，并批量提交元数据
- 对于**大块且对齐的写入**，会走直接写路径（pwrite），绕过缓存，降低拷贝与页管理开销
- FUSE 挂载启用 `writeback_cache` 与 `async`，并在 `init` 中将 `max_write` 提升到 16MB

因此数据缓存不再是纯“写穿”策略，而是典型的 writeback 模式。

### 元数据引擎

~~在`junkfs`中，目前仅使用了`sled`作为元数据存储引擎，但在实现时考虑了扩展性，其他的引擎只需要实现`MetaStore`
trait即可替换掉`sled`，存储引擎只需要提供`key-value`接口即可，比如存储引擎为关系式数据库，那么对于`ls`
命令，可能的操作是 `select * from dentry_table where dentry_name like 'd_233_%'`~~

目前已经改为使用 `mace` 作为元数据引擎，并且不考虑扩展。所有元数据存放在固定 bucket 中，
`mknod/unlink/rename/link` 等操作会**先写入 pending 缓冲**，再由后台线程或 `fsync` 批量提交。
`commit_pending` 内部使用单事务提交多个 key，确保这批更新的原子性。后台线程按**阈值或时间间隔**
提交（默认阈值约 8K key、间隔 200ms），以减少事务开销。
