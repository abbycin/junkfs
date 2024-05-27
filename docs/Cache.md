## The Cache Design

the cache module is based on key-value abstraction by `trait MetaOps`, there are 3 kind of items to be cached in  
our system: `SuperBlock`、`Dentry`、`Inode`, these items are persist by backend (such as: Redis, RocksDB and others)  
in our case it's `Sled`

the` trait MetaOps` is used for extending backend storage, currently, only `Sled` is supported  

since there are 3 kind of cache items, to simply implementation, we use trait object to unify all those kinds by   
`trait CacheItem` which is fit to LRU cache implementation

```rust
trait CacheItem {}
```

------

## TODO

the `Meta` in `meta.rs` is a wrapper of `backend` implements, such as `SledStore` in `sled.rs`  

the concrete backend hold a `LRUCache` and a `real backend` that do persistence works and it  
implements `trait MetaStore`

### works to be done
- refactor `meta.rs` replace `meta` member to `dyn MetaStore`
- add cache operation in `impl MetaStore for SledStore`
- test