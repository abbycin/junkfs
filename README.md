## A toy filesystem based on FUSE

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

## TODO

- [ ] `du` support
- [ ] `file lock` and concurrency support
- [ ] remote `MetaData` and `File` storage