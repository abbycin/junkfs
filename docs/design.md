这是一个`Rust`和`FUSE`的练习项目，目前已实现数个常用的`POSIX`文件操作方法，这里介绍`junkfs`的设计

### 元数据设计

在`junkfs`中，元数据序列化后存在`kvdb`中（使用`sled`实现），因此在格式化时需要提供 `kvdb`的存储路径，同时`junkfs`
旨在做本地的文件系统，因此，在格式化时还需要将数据存储路径写入到元数据中

在`junkfs`中，元数据分为

1. `SuperBlock`
1. `Dentry`
2. `Inode`

#### SuperBlock

由于使用了`kvdb`在`superblock`中可以将`dentry`剥离出来，由于`dentry`是一种特殊的文件，因此也会占有`inode`
，因此在`superblock`中只保持`inode`空闲管理的`bitmap`，同时存储数据存储路径，`superblock`结构如下

```rust
struct SuperBlock {
    store_path: String,
    imap: BitMap,
}
```

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

在`junkfs`中，数据会按照固定块大小拆分，存放在格式化是设置的目录下，对一个文件来说存储格式为`$ino/$blk_id`，即在目录为`$ino`
下存放名字为`$blk_id`的数据块。这里和本地文件系统不同，本地文件系统会在`inode`中记录数据块的位置和大小，在`junkfs`
中则没有这么做，因为`junkfs`的文件存储不是基于块设备的，而是基于本地文件系统，文件可以看作是平坦的连续的内存，但在基于块设备实现的本地文件系统中本身就需要完成分散块到文件的连续逻辑地址的映射，因此必须记录块的属性

#### 文件抽象

在`junkfs`中也有类似于内核中的`struct file`结构，这个结构就是`FileHandle`，它包含一个全局位于的`id`
，在文件时分配，关闭时释放，一个文件可以打开多次，因此存在一个`inode`对应多个`FileHandle`的情况

在`FileHandle`中实现了`read`、`write`和`flush`功能，这样是对照`struct file`设计的，`Inode`负责结构管理，`FileHandle`负责内容管理

#### 目录抽象

在`junkfs`中目录和文件是独立的实现，但它们都实现了一个比较的`trait`，这样它们就可以存放在同一个以`ino`
为`key`，`Box<dyn Trait>`为值的表中，统一资源管理。同样，和`FileHandle`一样，目录结构`DirHandle`中也有一个全局唯一的`id`
，这个结构的作用是：在列出目录内容时保证原子性。即在打开目录时，将目录内容读取出来，在列出内容时向外吐出读取的目录项，这样可以避免当目录在读取同时又有新建或删除操作在目录下进行，导致列出的目录项出现重复或者丢失乱序等问题（这也是`POSIX`
设计的问题）

### 缓存

在`junkfs`中有两类缓存：1. 元数据缓存，2. 数据缓存

##### 元数据缓存

元数据缓存包括`dentry`和`inode`采用`LRU`淘汰算法，设计上是支持`write back`的，但在实现上确实`write through`，对于`ls`
这种极度常用的命令，如果采用`write back`模式，每次都需要从`kvdb`读取`dentry`后再和缓存对比去重取新，实现稍显复杂，因此元数据缓存仅作为读缓存使用

##### 数据缓存

数据缓存使用固定大小的页面组成，在设计上，当缓存不足时将缓存刷到文件中，或在刷写超时后刷到文件。同样在实现中每当文件关闭时或读取前都会将缓存刷到文件中，原因是`junkfs`
没有定时任务支持，同时对于同一个文件描述符写后读场景有限，结果就是数据缓存显得很鸡肋。如果后续后续有了定时任务支持，那么就可以实现写数据到缓存后立即返回，提高写性能。在`Rust`
中实现还是有点困难，尤其是`fuser`这个`crate`本身不支持`async`这一套（[这里](https://github.com/jmpq/async-fuse-rs)
倒是有个异步实现），如果使用额外线程来实现，那数据和代码结构将会非常复杂。

### 元数据引擎

在`junkfs`中，目前仅使用了`sled`作为元数据存储引擎，但在实现时考虑了扩展性，其他的引擎只需要实现`MetaStore`
trait即可替换掉`sled`，存储引擎只需要提供`key-value`接口即可，比如存储引擎为关系式数据库，那么对于`ls`
命令，可能的操作是 `select * from dentry_table where dentry_name like 'd_233_%'`