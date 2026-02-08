use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

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

struct WriteStats {
    write_calls: AtomicU64,
    write_bytes: AtomicU64,
    dirty_bytes: AtomicI64,
    flush_calls: AtomicU64,
    flush_bytes: AtomicU64,
    flush_ns: AtomicU64,
    flush_errors: AtomicU64,
    pwritev_calls: AtomicU64,
    pwritev_bytes: AtomicU64,
    pwritev_ns: AtomicU64,
}

impl WriteStats {
    const fn new() -> Self {
        Self {
            write_calls: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            dirty_bytes: AtomicI64::new(0),
            flush_calls: AtomicU64::new(0),
            flush_bytes: AtomicU64::new(0),
            flush_ns: AtomicU64::new(0),
            flush_errors: AtomicU64::new(0),
            pwritev_calls: AtomicU64::new(0),
            pwritev_bytes: AtomicU64::new(0),
            pwritev_ns: AtomicU64::new(0),
        }
    }
}

static WRITE_STATS: WriteStats = WriteStats::new();

pub(crate) fn record_write(bytes: usize) {
    if bytes == 0 {
        return;
    }
    WRITE_STATS.write_calls.fetch_add(1, Ordering::Relaxed);
    WRITE_STATS.write_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    WRITE_STATS.dirty_bytes.fetch_add(bytes as i64, Ordering::Relaxed);
}

pub(crate) fn record_flush(bytes: u64, ns: u64, ok: bool) {
    if bytes == 0 {
        return;
    }
    WRITE_STATS.flush_calls.fetch_add(1, Ordering::Relaxed);
    WRITE_STATS.flush_bytes.fetch_add(bytes, Ordering::Relaxed);
    WRITE_STATS.flush_ns.fetch_add(ns, Ordering::Relaxed);
    WRITE_STATS.dirty_bytes.fetch_sub(bytes as i64, Ordering::Relaxed);
    if !ok {
        WRITE_STATS.flush_errors.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn record_pwritev(bytes: u64, ns: u64) {
    if bytes == 0 {
        return;
    }
    WRITE_STATS.pwritev_calls.fetch_add(1, Ordering::Relaxed);
    WRITE_STATS.pwritev_bytes.fetch_add(bytes, Ordering::Relaxed);
    WRITE_STATS.pwritev_ns.fetch_add(ns, Ordering::Relaxed);
}

pub(crate) fn snapshot() -> StatsSnapshot {
    StatsSnapshot {
        write_calls: WRITE_STATS.write_calls.load(Ordering::Relaxed),
        write_bytes: WRITE_STATS.write_bytes.load(Ordering::Relaxed),
        dirty_bytes: WRITE_STATS.dirty_bytes.load(Ordering::Relaxed),
        flush_calls: WRITE_STATS.flush_calls.load(Ordering::Relaxed),
        flush_bytes: WRITE_STATS.flush_bytes.load(Ordering::Relaxed),
        flush_ns: WRITE_STATS.flush_ns.load(Ordering::Relaxed),
        flush_errors: WRITE_STATS.flush_errors.load(Ordering::Relaxed),
        pwritev_calls: WRITE_STATS.pwritev_calls.load(Ordering::Relaxed),
        pwritev_bytes: WRITE_STATS.pwritev_bytes.load(Ordering::Relaxed),
        pwritev_ns: WRITE_STATS.pwritev_ns.load(Ordering::Relaxed),
    }
}
