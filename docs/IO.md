## The IO procedure

#### File
file is associated by `Ino` and `FileHandle`, a file has only one `Ino` but can be opened  
several times, thus produce multiple `FileHandle`

each `FileHandle` will hold its own cache (currently no read cache support), for `write`  
all data is first copy into cache, until cache timeout or use manually call `flush` or  
file is closed

when file is opened multiple times, the user is required to use `file lock` to ensure file  
consistency. in my implementation, when `file lock` is set, I will flush cache everytime when  
write was finished

#### Write
a `write` is always copy to cache, the cache has structure list below
```rust
struct Entry {
    blk_id: u64, // block id of a file
    off: u64,   // offset in block
    size: u64    // length of current write data
}
```
the `Entry` is cached in a `Vec` ordered as they are written

when a `write` is finished, or cache limit reached, the `Enrty` list will be flush to blocks, the  
blocks maybe already exists or need to be created

when perform `flush`, the `Entries` need to be merged, if there are holes, they will be filled  
with 0


#### Read
`read` is not cached, so it's always load data from blocks, when there's `write` coexist with `read`  
the user is required to use `file lock` to ensure consistence

- if the `read` request `offset` and `len` has no block yet, it should fill zeroed-buffer
- if the `read` is reach file's end, it should return 0

#### Relations

- `MemPool` is a global PAGE manager
- `FileHandle` contains an int fd and a `CacheStore`
- `CacheStore` contains a list of `Entry` and a `dyn Store` pointer

in write operation
- first find `FileHandle` by `ino` and `fh`
- then copy data into `CacheStore`
- when `CacheStore` limit reaches, flush data to `dyn Store`