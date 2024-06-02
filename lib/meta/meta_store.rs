pub struct MetaIter {
    pub iter: Box<dyn Iterator<Item = Option<Vec<u8>>>>,
}

pub trait MetaStore {
    fn insert(&self, key: &str, val: &[u8]) -> Result<(), String>;

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;

    fn scan_prefix(&self, prefix: &str) -> MetaIter;

    fn remove(&self, key: &str) -> Result<(), String>;

    fn contains_key(&self, key: &str) -> Result<bool, String>;

    fn flush(&self);
}

impl MetaIter {
    pub fn next(&mut self) -> Option<Vec<u8>> {
        self.iter.next().unwrap_or_else(|| None)
    }
}
