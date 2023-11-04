use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

use crate::algos::{Replica, SOURCE_HYPERVERTEX, Unique, UNIQUES_AS_HYPEREDGES};

pub static COMMUTE: fn(u32, u32) -> u32 = u32::min;

pub struct SPV1 {
    distance: AtomicU32,
    length: u32,
    local_out_neighbors: Vec<u32>,
}

impl Debug for SPV1 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SPV1")
            .field("dist", &self.distance.load(Relaxed))
            .finish()
    }
}

impl Replica<4, 4, u32, u32, u32> for SPV1 {
    fn new(old_id: u32, _: u32, local_out_neighbors: Vec<u32>, _: u32, weigh: Option<&str>) -> (Self, bool) {
        let uniques_as_hyperedges;
        unsafe { uniques_as_hyperedges = UNIQUES_AS_HYPEREDGES };
        let length: u32 = if uniques_as_hyperedges {
            0
        } else {
            weigh.map(|s| s.parse().expect(format!("Could not parse weigh {s:?} to a u32.").as_str())).unwrap_or(1)
        };
        let source_hypervertex;
        unsafe { source_hypervertex = SOURCE_HYPERVERTEX };
        let active;
        let dist: u32;
        if uniques_as_hyperedges && old_id == source_hypervertex {
            dist = 0;
            active = true;
        } else {
            active = false;
            dist = u32::MAX;
        };
        (Self {
            distance: AtomicU32::new(dist),
            length,
            local_out_neighbors,
        }, active)
    }

    fn update(&self, send_msg: impl Fn(u32, u32), _: u16) -> bool {
        let msg = self.distance.load(Relaxed) + self.length;
        for neighbour in &self.local_out_neighbors {
            send_msg(*neighbour, msg);
        }
        false
    }


    fn rcv_msg(&self, msg: u32) -> bool {
        let old_val = self.distance.fetch_min(msg, Relaxed);
        if msg < old_val {
            true
        } else {
            false
        }
    }

    fn get_status(&self) -> u32 {
        self.distance.load(Relaxed)
    }

    fn rcv_status(&self, status: u32) {
        self.distance.store(status, Relaxed);
    }
}

pub struct SPV2 {
    distance: AtomicU32,
    length: u32,
    out_neighbors: Vec<u32>,
}

impl Unique<4, u32, u32> for SPV2 {
    fn new(id: u32, out_neighbors: Vec<u32>, weigh: Option<&str>) -> (Self, bool) {
        let uniques_as_hyperedges;
        unsafe { uniques_as_hyperedges = UNIQUES_AS_HYPEREDGES };
        let length: u32 = if !uniques_as_hyperedges {
            0
        } else {
            weigh.map(|s| s.parse().expect(format!("Could not parse weigh {s:?} to a u32.").as_str())).unwrap_or(1)
        };
        let source_hypervertex;
        unsafe { source_hypervertex = SOURCE_HYPERVERTEX };
        let active;
        let dist: u32;
        if !uniques_as_hyperedges && id == source_hypervertex {
            dist = 0;
            active = true;
        } else {
            active = false;
            dist = u32::MAX;
        };
        (Self {
            distance: AtomicU32::new(dist),
            length,
            out_neighbors,
        }, active)
    }

    fn update(&self, send_msg: impl Fn(u32, u32), _: u16) -> bool {
        let msg = self.distance.load(Relaxed) + self.length;
        for neighbour in &self.out_neighbors {
            send_msg(*neighbour, msg);
        }
        false
    }


    fn rcv_msg(&self, msg: u32) -> bool {
        let old_val = self.distance.fetch_min(msg + self.length, Relaxed);
        if msg < old_val {
            true
        } else { false }
    }
}