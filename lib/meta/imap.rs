use crate::meta::Ino;
use crate::utils::BitMap64;

pub struct AllocPlan {
    pub ino: Ino,
    pub gid: u64,
    group: BitMap64,
    summary: BitMap64,
    group_cursor: u64,
    summary_cursor: u64,
}

pub struct FreePlan {
    pub gid: u64,
    group: BitMap64,
    summary: BitMap64,
    group_cursor: u64,
    summary_cursor: u64,
}

impl AllocPlan {
    pub fn summary_val(&self) -> Vec<u8> {
        bincode::serialize(&self.summary).expect("can't serialize imap summary")
    }

    pub fn group_val(&self) -> Vec<u8> {
        bincode::serialize(&self.group).expect("can't serialize imap group")
    }
}

#[derive(Clone)]
pub struct InoMap {
    total_inodes: u64,
    group_size: u64,
    group_count: u64,
    summary: BitMap64,
    groups: Vec<Option<BitMap64>>,
    summary_cursor: u64,
    group_cursor: Vec<u64>,
}

impl InoMap {
    pub fn new(total_inodes: u64, group_size: u64) -> Self {
        assert!(total_inodes > 0);
        assert!(group_size > 0);
        assert!(group_size.is_multiple_of(64));
        let group_count = total_inodes.div_ceil(group_size);
        let mut summary = BitMap64::new(group_count);
        for gid in 0..group_count {
            summary.set(gid);
        }
        let mut groups = Vec::with_capacity(group_count as usize);
        for gid in 0..group_count {
            let cap = Self::group_cap_by(total_inodes, group_size, gid);
            groups.push(Some(BitMap64::new(cap)));
        }
        let group_cursor = vec![0u64; group_count as usize];
        Self {
            total_inodes,
            group_size,
            group_count,
            summary,
            groups,
            summary_cursor: 0,
            group_cursor,
        }
    }

    pub fn from_summary(total_inodes: u64, group_size: u64, summary: BitMap64) -> Self {
        let group_count = total_inodes.div_ceil(group_size);
        let groups = vec![None; group_count as usize];
        let group_cursor = vec![0u64; group_count as usize];
        Self {
            total_inodes,
            group_size,
            group_count,
            summary,
            groups,
            summary_cursor: 0,
            group_cursor,
        }
    }

    pub fn group_count(&self) -> u64 {
        self.group_count
    }

    pub fn summary_key() -> String {
        "imap_sum".to_string()
    }

    pub fn group_key(gid: u64) -> String {
        format!("imap_{}", gid)
    }

    pub fn summary_val(&self) -> Vec<u8> {
        bincode::serialize(&self.summary).expect("can't serialize imap summary")
    }

    pub fn group_val(&self, gid: u64) -> Vec<u8> {
        let group = self.groups[gid as usize].as_ref().expect("imap group not loaded");
        bincode::serialize(group).expect("can't serialize imap group")
    }

    pub fn check(&self) {
        assert_eq!(self.summary.cap(), self.group_count);
        assert_eq!(self.groups.len() as u64, self.group_count);
        for (gid, g) in self.groups.iter().enumerate() {
            let gid = gid as u64;
            if let Some(g) = g {
                let cap = Self::group_cap_by(self.total_inodes, self.group_size, gid);
                assert_eq!(g.cap(), cap);
                assert_eq!(self.summary.test(gid), !g.full());
            }
        }
    }

    pub fn reserve(&mut self, ino: Ino) {
        if ino >= self.total_inodes {
            return;
        }
        let (gid, bit) = self.split(ino);
        if let Some(group) = &mut self.groups[gid as usize] {
            let was_full = group.full();
            if group.set(bit) && !was_full && group.full() {
                self.summary.clear(gid);
            }
        }
    }

    pub fn alloc_plan<F>(&mut self, loader: &mut F) -> Result<Option<AllocPlan>, String>
    where
        F: FnMut(u64) -> Result<BitMap64, String>,
    {
        if self.group_count == 0 || self.summary.is_empty() {
            return Ok(None);
        }
        let mut summary = self.summary.clone();
        let mut start_gid = self.summary_cursor;
        for _ in 0..self.group_count {
            let gid = match summary.find_one_from(start_gid).or_else(|| summary.find_one_from(0)) {
                Some(x) => x,
                None => return Ok(None),
            };
            self.ensure_group(gid, loader)?;
            let group = self.groups[gid as usize].as_ref().expect("imap group not loaded");
            let gcap = group.cap();
            if gcap == 0 {
                summary.clear(gid);
                start_gid = if gid + 1 >= self.group_count { 0 } else { gid + 1 };
                continue;
            }
            let start = if self.group_cursor[gid as usize] >= gcap { 0 } else { self.group_cursor[gid as usize] };
            let bit = match group.find_zero_from(start).or_else(|| group.find_zero_from(0)) {
                Some(x) => x,
                None => {
                    summary.clear(gid);
                    start_gid = if gid + 1 >= self.group_count { 0 } else { gid + 1 };
                    continue;
                }
            };
            let mut new_group = group.clone();
            new_group.set(bit);
            if new_group.full() {
                summary.clear(gid);
            }
            let group_cursor = if bit + 1 >= gcap { 0 } else { bit + 1 };
            let summary_cursor = if gid + 1 >= self.group_count { 0 } else { gid + 1 };
            return Ok(Some(AllocPlan {
                ino: gid * self.group_size + bit,
                gid,
                group: new_group,
                summary,
                group_cursor,
                summary_cursor,
            }));
        }
        Ok(None)
    }

    pub fn apply_alloc(&mut self, plan: AllocPlan) {
        let gid = plan.gid as usize;
        self.groups[gid] = Some(plan.group);
        self.summary = plan.summary;
        self.group_cursor[gid] = plan.group_cursor;
        self.summary_cursor = plan.summary_cursor;
    }

    pub fn free_plan<F>(&mut self, ino: Ino, loader: &mut F) -> Result<Option<FreePlan>, String>
    where
        F: FnMut(u64) -> Result<BitMap64, String>,
    {
        if ino == 0 || ino >= self.total_inodes {
            return Ok(None);
        }
        let (gid, bit) = self.split(ino);
        self.ensure_group(gid, loader)?;
        let group = self.groups[gid as usize].as_ref().expect("imap group not loaded");
        if !group.test(bit) {
            return Ok(None);
        }
        let was_full = group.full();
        let mut new_group = group.clone();
        new_group.clear(bit);
        let mut new_summary = self.summary.clone();
        if was_full {
            new_summary.set(gid);
        }
        Ok(Some(FreePlan {
            gid,
            group: new_group,
            summary: new_summary,
            group_cursor: bit,
            summary_cursor: gid,
        }))
    }

    pub fn apply_free(&mut self, plan: FreePlan) {
        let gid = plan.gid as usize;
        self.groups[gid] = Some(plan.group);
        self.summary = plan.summary;
        self.group_cursor[gid] = plan.group_cursor;
        self.summary_cursor = plan.summary_cursor;
    }

    pub fn summary(&self) -> &BitMap64 {
        &self.summary
    }

    pub fn replace_summary(&mut self, summary: BitMap64) {
        self.summary = summary;
        self.summary_cursor = 0;
    }

    fn split(&self, ino: Ino) -> (u64, u64) {
        let gid = ino / self.group_size;
        let bit = ino % self.group_size;
        (gid, bit)
    }

    fn group_cap_by(total_inodes: u64, group_size: u64, gid: u64) -> u64 {
        let start = gid * group_size;
        let end = std::cmp::min(total_inodes, start + group_size);
        end - start
    }

    fn ensure_group<F>(&mut self, gid: u64, loader: &mut F) -> Result<(), String>
    where
        F: FnMut(u64) -> Result<BitMap64, String>,
    {
        if self.groups[gid as usize].is_none() {
            let group = loader(gid)?;
            self.groups[gid as usize] = Some(group);
        }
        Ok(())
    }
}
