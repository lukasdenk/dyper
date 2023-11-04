use std::ops::Add;
use std::sync::atomic::Ordering::Relaxed;

use atomic_float::AtomicF64;

use crate::algos::{Replica, Unique, UNIQUES_AS_HYPEREDGES};

static ALPHA: f64 = 0.15;

pub static COMBINE: fn(f64, f64) -> f64 = f64::add;

pub struct PageRankReplica {
    importance: AtomicF64,
    local_out_neighbors: Vec<u32>,
    total_out_neighborhood_size: u32,
}

impl Replica<8, 8, f64, f64, f64> for PageRankReplica {
    fn new(_: u32, _: u32, local_out_neighbors: Vec<u32>, total_out_neighborhood_size: u32, _: Option<&str>) -> (Self, bool) {
        let uniques_as_hyperedges;
        unsafe { uniques_as_hyperedges = UNIQUES_AS_HYPEREDGES };
        let importance = if uniques_as_hyperedges {
            1.0
        } else {
            0.0
        };
        (Self {
            importance: AtomicF64::new(importance),
            local_out_neighbors,
            total_out_neighborhood_size,
        },
         true)
    }

    fn update(&self, send_msg: impl Fn(u32, f64), _: u16) -> bool {
        let uniques_as_hyperedges;
        unsafe { uniques_as_hyperedges = UNIQUES_AS_HYPEREDGES };
        if self.total_out_neighborhood_size != 0 {
            let msg = if uniques_as_hyperedges {
                (ALPHA + (1.0 - ALPHA) * self.importance.load(Relaxed)) / (self.total_out_neighborhood_size as f64)
            } else {
                self.importance.load(Relaxed) / (self.total_out_neighborhood_size as f64)
            };
            for id in &self.local_out_neighbors {
                send_msg(*id, msg);
            }
            self.importance.store(0.0, Relaxed);
        }
        true
    }

    fn rcv_msg(&self, msg: f64) -> bool {
        self.importance.fetch_add(msg, Relaxed);
        true
    }

    fn get_status(&self) -> f64 {
        self.importance.load(Relaxed)
    }

    fn rcv_status(&self, status: f64) {
        self.importance.store(status, Relaxed);
    }
}

unsafe impl Send for PageRankReplica {}

unsafe impl Sync for PageRankReplica {}

pub struct PageRankUnique {
    importance: AtomicF64,
    out_neighbors: Vec<u32>,
}

impl Unique<8, f64, f64> for PageRankUnique {
    fn new(_: u32, out_neighbors: Vec<u32>, _: Option<&str>) -> (Self, bool) {
        let uniques_as_hyperedges;
        unsafe { uniques_as_hyperedges = UNIQUES_AS_HYPEREDGES };
        let importance = if uniques_as_hyperedges {
            0.0
        } else {
            1.0
        };
        (Self {
            importance: AtomicF64::new(importance),
            out_neighbors,
        },
         true)
    }

    fn update(&self, send_msg: impl Fn(u32, f64), _: u16) -> bool {
        let uniques_as_hyperedges;
        unsafe { uniques_as_hyperedges = UNIQUES_AS_HYPEREDGES };
        if !self.out_neighbors.is_empty() {
            let msg = if !uniques_as_hyperedges {
                (ALPHA + (1.0 - ALPHA) * self.importance.load(Relaxed)) / (self.out_neighbors.len() as f64)
            } else {
                self.importance.load(Relaxed) / (self.out_neighbors.len() as f64)
            };
            for id in &self.out_neighbors {
                send_msg(*id, msg);
            }
            self.importance.store(0.0, Relaxed);
        }
        true
    }


    fn rcv_msg(&self, msg: f64) -> bool {
        self.importance.fetch_add(msg, Relaxed);
        true
    }
}

unsafe impl Send for PageRankUnique {}

unsafe impl Sync for PageRankUnique {}

