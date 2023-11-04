use dashmap::DashMap;

use crate::algos::{Replica, Unique};

pub struct LpReplica {
    label_to_frequency: DashMap<u32, u32>,
    local_out_neighbors: Vec<u32>,
}

fn key_by_max_value(map: &DashMap<u32, u32>) -> u32 {
    *map.iter().max_by_key(|entry| *entry.value()).expect("map is empty. Maybe there is a hypervertex without incoming hyperedges. Remove the hypervertex and restart.").key()
}

fn increment(frequency_map: &DashMap<u32, u32>, key: u32) {
    frequency_map.entry(key).and_modify(|frequency| { *frequency += 1; }).or_insert(1);
}


impl Replica<4, 4, u32, u32, u32> for LpReplica {
    fn new(_: u32, id: u32, local_out_neighbors: Vec<u32>, _: u32, _: Option<&str>) -> (Self, bool) {
        let label_to_frequency = DashMap::new();
        label_to_frequency.insert(id, 1);

        (Self {
            label_to_frequency,
            local_out_neighbors,
        }, true)
    }


    fn update(&self, send_msg: impl Fn(u32, u32), _: u16) -> bool {
        if !self.label_to_frequency.is_empty() {
            let msg = key_by_max_value(&self.label_to_frequency);
            for loc_neighbor in &self.local_out_neighbors {
                send_msg(*loc_neighbor, msg);
            }
            self.label_to_frequency.clear();
        }
        true
    }

    fn rcv_msg(&self, msg: u32) -> bool {
        increment(&self.label_to_frequency, msg);
        true
    }

    fn get_status(&self) -> u32 {
        key_by_max_value(&self.label_to_frequency)
    }

    fn rcv_status(&self, status: u32) {
        increment(&self.label_to_frequency, status);
    }
}


pub struct LpUnique {
    label_to_frequency: DashMap<u32, u32>,
    out_neighbors: Vec<u32>,
}


impl Unique<4, u32, u32> for LpUnique {
    fn new(_: u32, out_neighbors: Vec<u32>, _: Option<&str>) -> (Self, bool) {
        (Self {
            label_to_frequency: DashMap::new(),
            out_neighbors,
        }, true)
    }


    fn update(&self, send_msg: impl Fn(u32, u32), _: u16) -> bool {
        let msg = key_by_max_value(&self.label_to_frequency);
        for neighbor in &self.out_neighbors {
            send_msg(*neighbor, msg);
        }
        self.label_to_frequency.clear();
        true
    }

    fn rcv_msg(&self, msg: u32) -> bool {
        increment(&self.label_to_frequency, msg);
        true
    }
}
