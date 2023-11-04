use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

use crate::algos::{Replica, Unique};

pub static COMMUTE: fn(u32, u32) -> u32 = u32::min;

pub struct CCV1 {
    value: AtomicU32,
    local_out_neighbors: Vec<u32>,
}

impl Replica<4, 4, u32, u32, u32> for CCV1 {
    fn new(_: u32, _: u32, local_out_neighbors: Vec<u32>, _: u32, _: Option<&str>) -> (Self, bool) {
        let max_out_neighbor_id = *local_out_neighbors.iter().max().unwrap_or(&u32::MAX);
        (Self {
            value: AtomicU32::new(max_out_neighbor_id),
            local_out_neighbors,
        }, true)
    }


    fn update(&self, send_msg: impl Fn(u32, u32), _: u16) -> bool {
        let val = self.value.load(Relaxed);
        for out_neighbour in &self.local_out_neighbors {
            send_msg(*out_neighbour, val);
        }
        false
    }


    fn rcv_msg(&self, msg: u32) -> bool {
        let old_val = self.value.fetch_max(msg, Relaxed);
        if old_val<msg {
            true
        } else {
            false
        }
    }

    fn get_status(&self) -> u32 {
        self.value.load(Relaxed)
    }

    fn rcv_status(&self, status: u32) {
        self.value.store(status, Relaxed);
    }
}

pub struct CCV2 {
    value: AtomicU32,
    out_neighbors: Vec<u32>,
}

impl Debug for CCV2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CCV2")
            .field("val", &self.value.load(Relaxed))
            .field("neighbours", &self.out_neighbors)
            .finish()
    }
}

impl Unique<4, u32, u32> for CCV2 {
    fn new(id: u32, out_neighbors: Vec<u32>, _: Option<&str>) -> (Self, bool) {
        (Self {
            value: AtomicU32::new(id),
            out_neighbors,
        }, true)
    }

    fn update(&self, send_msg: impl Fn(u32, u32), _: u16) -> bool {
        let val = self.value.load(Relaxed);
        for neighbour in &self.out_neighbors {
            send_msg(*neighbour, val);
        }
        false
    }


    fn rcv_msg(&self, msg: u32) -> bool {
        let old_val = self.value.fetch_max(msg, Relaxed);
        if old_val<msg {
            true
        } else { false }
    }
}