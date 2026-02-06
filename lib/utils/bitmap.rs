use crate::utils::align_up;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BitMap {
    data: Vec<u8>,
    size: u64,
    count: u64,
}

const fn roundup_align8(size: u64) -> u64 {
    align_up(size, 8)
}

impl BitMap {
    pub fn new(cnt: u64) -> Self {
        let up = roundup_align8(cnt);
        // the bytes << 3 may greater than cnt, say cnt is 9, bytes << 3 will be 16
        let bytes = up >> 3;
        assert!((bytes << 3) >= cnt);
        Self {
            data: vec![0u8; bytes as usize],
            size: cnt,
            count: 0,
        }
    }

    pub fn add(&mut self, bit: u64) -> bool {
        if self.test(bit) || self.full() {
            return false;
        }

        self.data[(bit >> 3) as usize] |= 1 << (bit & 7);
        self.count += 1;
        true
    }

    pub fn test(&self, bit: u64) -> bool {
        (self.data[(bit >> 3) as usize] & (1 << (bit & 7))) != 0
    }

    pub fn del(&mut self, bit: u64) {
        self.data[(bit >> 3) as usize] &= !(1 << (bit & 7));
        self.count -= 1;
    }

    pub fn len(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn cap(&self) -> u64 {
        self.size
    }

    pub fn full(&self) -> bool {
        self.count == self.size
    }

    pub fn alloc(&mut self) -> Option<u64> {
        if self.full() {
            None
        } else {
            for i in 0..self.cap() {
                if !self.test(i) {
                    self.add(i);
                    return Some(i);
                }
            }
            None
        }
    }

    pub fn free(&mut self, bit: u64) -> bool {
        if !self.test(bit) {
            return false;
        }
        self.del(bit);
        true
    }
}

#[cfg(test)]
mod test {
    use super::BitMap;

    #[test]
    fn test_bitmap() {
        let mut bm = BitMap::new(1024);

        bm.add(233);
        bm.add(666);

        assert!(bm.test(233));
        assert!(bm.test(666));
        assert!(!bm.test(101));

        bm.del(233);
        assert!(!bm.test(233));
        assert_eq!(bm.len(), 1);
    }

    #[test]
    fn test_bitmap_s11n() {
        let mut bm = BitMap::new(1024);
        let mut bm_cnt = 0;

        for i in 233..666 {
            if i % 2 == 0 {
                bm_cnt += 1;
                bm.add(i);
            }
        }

        let r = bincode::serialize(&bm).expect("can't serialize");
        let d = bincode::deserialize::<BitMap>(r.as_slice()).expect("can't deserialize");

        assert_eq!(d.data.len(), bm.data.len());
        assert_eq!(d.len(), bm.len());
        assert_eq!(d.cap(), bm.cap());

        let mut d_cnt = 0;

        for i in 233..666 {
            if i % 2 == 0 {
                assert!(d.test(i));
                d_cnt += 1;
            }
        }

        assert_eq!(bm_cnt, d_cnt);
    }
}
