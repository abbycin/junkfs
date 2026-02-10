# junkfs

一个基于 **libfuse3 low-level C API** 的 Rust 实验型文件系统。  
它最初是 FUSE 练习项目，目前主要作为 `mace` 元数据引擎的测试载体。

详细设计见：`docs/design.md`

## 特性概览

- FUSE 多线程会话循环（`fuse_session_loop_mt`）
- 元数据存储：`mace-kv`
- 数据存储：每 inode 一个数据文件，按两级目录分片
- writeback 数据路径 + 后台写回线程
- 支持 open 文件延迟删除（更接近 Linux 语义）

## 依赖

- Linux
- `fuse3` 运行库与开发头文件（例如 `fuse3`, `fuse3-devel` / `libfuse3-dev`）
- 可访问的挂载点目录

## 已实现的主要操作

- `lookup`
- `getattr` / `setattr`
- `create` / `mknod` / `open` / `release`
- `read` / `write` / `flush`
- `mkdir` / `opendir` / `readdir` / `releasedir`
- `unlink` / `rmdir` / `rename`
- `link` / `symlink` / `readlink`
- `fsync` / `fsyncdir`

> 注意：这是实验型文件系统，不追求完整 POSIX 兼容。

## 快速开始

### 1) 格式化

`mkfs` 会清空并重建 `meta_path` 与 `store_path`。

```bash
cargo run --bin mkfs --release -- /nvme/meta /nvme/store
```

### 2) 挂载

```bash
mkdir -p ~/jfs
cargo run --bin junkfs --release -- /nvme/meta ~/jfs
```

默认日志输出到 `/tmp/junkfs.log`。

### 3) 使用

在另一个终端对 `~/jfs` 正常执行文件操作即可，例如：

```bash
tar xf /home/neo/Downloads/linux-6.12.69.tar.xz -C ~/jfs
cd ~/jfs/linux-6.12.69
make alldefconfig
make -j4
```

### 4) 卸载

```bash
umount ~/jfs
```

## 常用环境变量

- `JUNK_LEVEL`：日志级别，默认 `ERROR`
- `JUNK_DISABLE_WBC=1`：关闭 FUSE writeback cache（默认开启）
- `JUNK_ENABLE_INO_REUSE=0|1`：控制 inode 复用（默认 `1`）
- `JUNK_STRICT_INVARIANT=1`：开启严格一致性断言（默认关闭）
- `JUNK_VERIFY_FLUSH=1`：开启写后校验（调试用，默认关闭）

## stats 统计（可选）

可通过 feature 打开写入统计日志：

```bash
cargo run --bin junkfs --release --features stats -- /nvme/meta ~/jfs
```

## 已知限制

- 默认 writeback 策略偏性能，崩溃一致性依赖 `fsync/fsyncdir`
- 元数据后端固定为 `mace-kv`
- 面向测试与实验，不建议作为生产文件系统直接使用
