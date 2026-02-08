mod cache_store;
mod filestore;
#[cfg(feature = "stats")]
mod stats;
#[cfg(not(feature = "stats"))]
mod stats {
    #[derive(Clone, Copy)]
    pub(crate) struct StatsSnapshot {
        pub write_calls: u64,
        pub write_bytes: u64,
        pub dirty_bytes: i64,
        pub flush_calls: u64,
        pub flush_bytes: u64,
        pub flush_ns: u64,
        pub flush_errors: u64,
        pub pwritev_calls: u64,
        pub pwritev_bytes: u64,
        pub pwritev_ns: u64,
    }

    pub(crate) fn record_write(_: usize) {}

    pub(crate) fn record_flush(_: u64, _: u64, _: bool) {}

    pub(crate) fn record_pwritev(_: u64, _: u64) {}

    pub(crate) fn snapshot() -> StatsSnapshot {
        StatsSnapshot {
            write_calls: 0,
            write_bytes: 0,
            dirty_bytes: 0,
            flush_calls: 0,
            flush_bytes: 0,
            flush_ns: 0,
            flush_errors: 0,
            pwritev_calls: 0,
            pwritev_bytes: 0,
            pwritev_ns: 0,
        }
    }
}

pub use cache_store::CacheStore;
pub use filestore::FileStore;
pub(crate) use stats::{record_flush, record_pwritev, record_write, snapshot, StatsSnapshot};

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) blk_id: u64,   // block id
    pub(crate) blk_off: u64,  // offset in block
    pub(crate) off: u64,      // global offset in file
    pub(crate) size: u64,     // data length
    pub(crate) data: *mut u8, // data buffer
}

unsafe impl Send for Entry {}
unsafe impl Sync for Entry {}
