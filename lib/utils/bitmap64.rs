use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BitMap64 {
    data: Vec<u64>,
    size: u64,
    count: u64,
}

impl BitMap64 {
    pub fn new(cnt: u64) -> Self {
        let words = if cnt == 0 { 0 } else { (cnt + 63) / 64 };
        Self {
            data: vec![0u64; words as usize],
            size: cnt,
            count: 0,
        }
    }

    pub fn set(&mut self, bit: u64) -> bool {
        if bit >= self.size {
            return false;
        }
        let idx = (bit >> 6) as usize;
        let mask = 1u64 << (bit & 63);
        if (self.data[idx] & mask) != 0 {
            return false;
        }
        self.data[idx] |= mask;
        self.count += 1;
        true
    }

    pub fn clear(&mut self, bit: u64) -> bool {
        if bit >= self.size {
            return false;
        }
        let idx = (bit >> 6) as usize;
        let mask = 1u64 << (bit & 63);
        if (self.data[idx] & mask) == 0 {
            return false;
        }
        self.data[idx] &= !mask;
        self.count -= 1;
        true
    }

    pub fn test(&self, bit: u64) -> bool {
        if bit >= self.size {
            return false;
        }
        let idx = (bit >> 6) as usize;
        (self.data[idx] & (1u64 << (bit & 63))) != 0
    }

    pub fn len(&self) -> u64 {
        self.count
    }

    pub fn cap(&self) -> u64 {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn full(&self) -> bool {
        self.count == self.size
    }

    pub fn find_zero_from(&self, start: u64) -> Option<u64> {
        if self.size == 0 {
            return None;
        }
        let start = if start >= self.size { 0 } else { start };
        let last_word = self.data.len().saturating_sub(1) as u64;
        let start_word = start >> 6;
        for wi in start_word..=last_word {
            let mut mask = !self.data[wi as usize];
            if wi == start_word {
                let bit = start & 63;
                let before = if bit == 0 { 0 } else { (1u64 << bit) - 1 };
                mask &= !before;
            }
            if wi == last_word {
                let tail = self.size & 63;
                if tail != 0 {
                    let valid = (1u64 << tail) - 1;
                    mask &= valid;
                }
            }
            if mask != 0 {
                let off = mask.trailing_zeros() as u64;
                return Some((wi << 6) + off);
            }
        }
        None
    }

    pub fn find_one_from(&self, start: u64) -> Option<u64> {
        if self.size == 0 {
            return None;
        }
        let start = if start >= self.size { 0 } else { start };
        let last_word = self.data.len().saturating_sub(1) as u64;
        let start_word = start >> 6;
        for wi in start_word..=last_word {
            let mut mask = self.data[wi as usize];
            if wi == start_word {
                let bit = start & 63;
                let before = if bit == 0 { 0 } else { (1u64 << bit) - 1 };
                mask &= !before;
            }
            if wi == last_word {
                let tail = self.size & 63;
                if tail != 0 {
                    let valid = (1u64 << tail) - 1;
                    mask &= valid;
                }
            }
            if mask != 0 {
                let off = mask.trailing_zeros() as u64;
                return Some((wi << 6) + off);
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::BitMap64;

    #[test]
    fn test_bitmap64_basic() {
        let mut bm = BitMap64::new(130);
        assert_eq!(bm.cap(), 130);
        assert!(bm.set(0));
        assert!(bm.set(129));
        assert!(bm.test(0));
        assert!(bm.test(129));
        assert!(!bm.test(128));
        assert!(bm.clear(0));
        assert!(!bm.test(0));
        assert_eq!(bm.len(), 1);
    }

    #[test]
    fn test_bitmap64_find() {
        let mut bm = BitMap64::new(130);
        bm.set(0);
        bm.set(1);
        bm.set(2);
        assert_eq!(bm.find_zero_from(0), Some(3));
        assert_eq!(bm.find_zero_from(3), Some(3));
        assert_eq!(bm.find_one_from(0), Some(0));
        assert_eq!(bm.find_one_from(3), None);
    }
}
