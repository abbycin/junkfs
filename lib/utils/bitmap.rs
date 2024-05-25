use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct BitMap {
    data: Vec<u8>,
    size: u64,
    count: u64,
}

const fn is_power_of2(size: u64) -> bool {
    (size > 0) && (size & (size - 1)) == 0
}

const fn roundup_align8(size: u64) -> u64 {
    (size + 7) & (!7)
}

impl BitMap {
    pub fn new(size: u64) -> Self {
        let size = roundup_align8(size >> 3);
        if !is_power_of2(size) {
            panic!("invalid size for bitmap, expect power of 2");
        }
        let mut v = Vec::with_capacity(size as usize);
        v.resize(size as usize, 0);
        Self {
            data: v,
            size: size * 8,
            count: 0,
        }
    }

    pub fn add(&mut self, bit: u64) -> bool {
        if self.test(bit) || self.count == self.size {
            return false;
        }

        self.data[(bit >> 3) as usize] |= 1 << (bit & 7);
        self.count += 1;
        return true;
    }

    pub fn test(&self, mut bit: u64) -> bool {
        if (self.data[(bit >> 3) as usize] & (1 << (bit & 7))) != 0 {
            true
        } else {
            false
        }
    }

    pub fn del(&mut self, bit: u64) {
        self.data[(bit >> 3) as usize] &= !(1 << (bit & 7));
        self.count -= 1;
    }

    pub fn len(&self) -> u64 {
        self.count
    }

    pub fn cap(&self) -> u64 {
        self.size
    }

    pub fn full(&self) -> bool {
        self.len() == self.cap()
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
