use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

use dashmap::DashSet;
use futures::future::join_all;
use rayon::iter::*;

use crate::algos::{Replica, ToFromBytes, Unique};
use crate::importer;
use crate::peer::{create_peers, Peer};

static MASTER_PARTITION: u8 = 0;
static ACTIVE_FACTOR: f64 = 0.05;

pub struct Hypergraph<const LM2: usize, const LS: usize, M1: Send + Sync + 'static, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>, R: Replica<LM2, LS, M1, M2, S>, U: Unique<LM2, M1, M2>> {
    replicas: HashMap<u32, (AtomicBool, R)>,
    uniques: HashMap<u32, (AtomicBool, U)>,
    master_replicas: HashMap<u32, Vec<u8>>,
    active_replicas: Option<DashSet<u32>>,
    active_uniques: Option<DashSet<u32>>,
    peers: HashMap<u8, Arc<Peer<LM2, LS, M2, S>>>,
    node_id: u8,
    phantom: std::marker::PhantomData<M1>,
}

impl<const LM2: usize, const LS: usize, M1: Send + Sync + 'static, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>, R: Replica<LM2, LS, M1, M2, S>, U: Unique<LM2, M1, M2>> Hypergraph<LM2, LS, M1, M2, S, R, U> {
    fn new(replicas: HashMap<u32, (AtomicBool, R)>, uniques: HashMap<u32, (AtomicBool, U)>, master_replicas: HashMap<u32, Vec<u8>>, peers: HashMap<u8, Arc<Peer<LM2, LS, M2, S>>>, node_id: u8) -> Self {
        let replicas_explicit_tracking_mode = Self::is_explicit_tracking_mode(&replicas);
        let active_replicas = if replicas_explicit_tracking_mode { Some(Self::get_ids_active_elements(&replicas)) } else { None };

        let uniques_explicit_tracking_mode = Self::is_explicit_tracking_mode(&uniques);
        let active_uniques = if uniques_explicit_tracking_mode { Some(Self::get_ids_active_elements(&uniques)) } else { None };
        println!("Active modes: replicas: {replicas_explicit_tracking_mode:?}, uniques: {uniques_explicit_tracking_mode:?}");

        Self {
            replicas,
            master_replicas,
            active_replicas,
            uniques,
            active_uniques,
            peers,
            node_id,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn replica_rcv_msg(&self, id: u32, msg: M2) {
        let (active, replica) = self.replicas.get(&id).unwrap();
        let activate = replica.rcv_msg(msg);
        if activate {
            if self.active_replicas.is_some() {
                if !active.load(Relaxed) {
                    self.active_replicas.as_ref().unwrap().insert(id);
                }
            }
            active.store(true, Relaxed);
        }
    }


    pub fn replica_rcv_status(&self, id: u32, status: S) {
        let (active, replica) = self.replicas.get(&id).unwrap();
        active.store(true, Relaxed);
        replica.rcv_status(status);
        if self.active_replicas.is_some() {
            self.active_replicas.as_ref().unwrap().insert(id);
        }
    }

    fn send_status_to_servant_replicas(&self, replica_id: u32, status: S, peer_ids: &Vec<u8>) {
        for peer_id in peer_ids {
            let peer = self.peers.get(peer_id).unwrap();
            peer.send_status(replica_id, status);
        }
    }

    pub fn broadcast_statuses(&self) {
        if let Some(active_replicas) = self.active_replicas.as_ref() {
            active_replicas.par_iter().for_each(|replica_id| {
                let (_, replica) = self.replicas.get(&*replica_id).unwrap();
                if let Some(peer_ids) = self.master_replicas.get(&*replica_id) {
                    self.send_status_to_servant_replicas(*replica_id, replica.get_status(), peer_ids);
                }
            });
        } else {
            self.master_replicas.par_iter().for_each(|(replica_id, peer_ids)| {
                let (active, replica) = self.replicas.get(replica_id).unwrap();
                if active.load(Relaxed) {
                    self.send_status_to_servant_replicas(*replica_id, replica.get_status(), peer_ids);
                }
            });
        }
    }

    pub fn update_active_replicas(&self, superstep: u16) -> usize {
        let active_replicas_at_phase1_start;
        if self.active_replicas.is_some() {
            let active_replicas = self.active_replicas.as_ref().unwrap();
            active_replicas_at_phase1_start = active_replicas.len();
            let new_active: DashSet<u32> = active_replicas.par_iter().filter(|replica_id| {
                let (active, replica) = self.replicas.get(&*replica_id).unwrap();
                let stay_active = replica.update(|unique_id, msg| self.unique_rcv_msg(unique_id, msg), superstep);
                if !stay_active {
                    active.store(false, Relaxed);
                }
                stay_active
            }).map(|replica_id| *replica_id).collect();
            active_replicas.clear();
            new_active.into_par_iter().for_each(|replica_id| { active_replicas.insert(replica_id); });
        } else {
            active_replicas_at_phase1_start = self.replicas.par_iter().map(|(_, (active, replica))|
                {
                    let active_beginning_phase1 = active.load(Relaxed);
                    if active_beginning_phase1 {
                        let stay_active = replica.update(|unique_id, msg| self.unique_rcv_msg(unique_id, msg), superstep);
                        if !stay_active {
                            active.store(false, Relaxed);
                        }
                    }
                    active_beginning_phase1
                }
            ).filter(|active_beginning_phase1| *active_beginning_phase1).count();
        }
        active_replicas_at_phase1_start
    }


    pub fn unique_rcv_msg(&self, id: u32, msg: M1) {
        let (active, unique) = self.uniques.get(&id).unwrap();
        let activate = unique.rcv_msg(msg);

        if activate {
            if self.active_uniques.is_some() && !active.load(Relaxed) {
                self.active_uniques.as_ref().unwrap().insert(id);
            }
            active.store(true, Relaxed);
        }
    }

    pub fn update_active_uniques(&self, superstep: u16) -> usize {
        let active_uniques_at_phase1_start;
        if self.active_uniques.is_some() {
            let active_uniques = self.active_uniques.as_ref().unwrap();
            active_uniques_at_phase1_start = active_uniques.len();
            let new_active: DashSet<u32> = active_uniques.par_iter().filter(|unique_id| {
                let (active, unique) = self.uniques.get(&*unique_id).unwrap();
                let stay_active = unique.update(|replica_id, msg| self.send_to_replica(replica_id, msg), superstep);
                if !stay_active {
                    active.store(false, Relaxed);
                }
                stay_active
            }).map(|unique_id| *unique_id).collect();
            active_uniques.clear();
            new_active.into_par_iter().for_each(|unique_id| { active_uniques.insert(unique_id); });
        } else {
            active_uniques_at_phase1_start = self.uniques.par_iter().map(|(_, (active, unique))|
                {
                    let active_beginning_phase1 = active.load(Relaxed);
                    if active_beginning_phase1 {
                        let stay_active = unique.update(|replica_id, msg| self.send_to_replica(replica_id, msg), superstep);
                        if !stay_active {
                            active.store(false, Relaxed);
                        }
                    }
                    active_beginning_phase1
                }
            ).filter(|active_beginning_phase1| *active_beginning_phase1).count();
        }
        active_uniques_at_phase1_start
    }

    fn send_to_replica(&self, id: u32, msg: M2) {
        let total_machines = (self.peers.len() + 1) as u32;
        let master_replica_peer = (id % total_machines) as u8;
        if master_replica_peer == self.node_id {
            self.replica_rcv_msg(id, msg);
        } else {
            let master_peer = self.peers.get(&master_replica_peer).unwrap();
            master_peer.send_msg(id, msg);
        }
    }

    fn is_active(&self) -> bool {
        if self.active_replicas.as_ref().map(|inner| !inner.is_empty()).unwrap_or(false) {
            return true;
        }
        if self.active_uniques.as_ref().map(|inner| !inner.is_empty()).unwrap_or(false) {
            return true;
        }
        if self.uniques.par_iter().find_any(|(_, (active, _))| active.load(Relaxed)).is_some() {
            return true;
        }
        self.replicas.par_iter().find_any(|(_, (active, _))| active.load(Relaxed)).is_some()
    }

    pub async fn run(mut self: Arc<Self>, max_supersteps: Option<u16>) {
        let mut replica_switch_explicit_tracking_mode_protection: u8 = 0;
        let mut unique_switch_explicit_tracking_mode_protection: u8 = 0;
        for superstep in 0..max_supersteps.unwrap_or(u16::MAX) {
            println!("STARTING SUPERSTEP {superstep}...");
            self.phase1(&mut replica_switch_explicit_tracking_mode_protection, superstep);

            self.phase2(&mut unique_switch_explicit_tracking_mode_protection, superstep).await;

            if !self.peers.is_empty() {
                //phase 3
                let hypergraph_clone = self.clone();
                let servant_replica_rcv = Arc::new(move |id, msg| hypergraph_clone.replica_rcv_status(id, msg));
                self.peers_start_phase3(servant_replica_rcv).await;
                self.broadcast_statuses();
                self.stop_peer_communication().await;
                self.hyper_graph_end_phase();
            }

            //phase 4
            let is_hypergraph_active = self.phase4().await;
            if !is_hypergraph_active {
                break;
            }

        }
    }

    fn phase1(self: &mut Arc<Self>, mut replica_switch_protection: &mut u8, superstep: u16) {
        let active_replicas_at_phase1_start = self.update_active_replicas(superstep);
        println!("replicas: active beginning phase 1: {}, total: {}", active_replicas_at_phase1_start, self.replicas.len());
        let updated = Self::update_explicit_tracking_mode(self.active_replicas.is_some(), &mut replica_switch_protection, &self.replicas, active_replicas_at_phase1_start);
        if updated {
            println!("switch replica active mode. New: {:?}", self.active_replicas.is_none());
            let this = Arc::get_mut(self).unwrap();
            if this.active_replicas.is_none() {
                let active_replicas = Self::get_ids_active_elements(&this.replicas);
                this.active_replicas = Some(active_replicas);
            } else {
                this.active_replicas = None;
            }
        }
        self.hyper_graph_end_phase();
    }

    async fn phase2(self: &mut Arc<Self>, explicit_tracking_mode_switch_protection: &mut u8, superstep: u16) {
        let hypergraph_clone = self.clone();
        let master_replica_rcv = Arc::new(move |id, msg| hypergraph_clone.replica_rcv_msg(id, msg));
        self.peers_start_phase2(master_replica_rcv).await;
        let active_uniques_start_phase2 = self.update_active_uniques(superstep);
        self.stop_peer_communication().await;
        println!("uniques: active beginning phase 2: {}, total: {}", active_uniques_start_phase2, self.uniques.len());
        let updated = Self::update_explicit_tracking_mode(self.active_uniques.is_some(), explicit_tracking_mode_switch_protection, &self.uniques, active_uniques_start_phase2);
        if updated {
            println!("switch unique active mode. New: {:?}", self.active_uniques.is_none());
            let this = Arc::get_mut(self).unwrap();
            if this.active_uniques.is_none() {
                this.active_uniques = Some(Self::get_ids_active_elements(&this.uniques));
            } else {
                this.active_uniques = None;
            }
        }
        self.hyper_graph_end_phase();
    }

    fn hyper_graph_end_phase(&self) {
        println!("\nActive replicas: {}", self.active_replicas.as_ref().map(|inner| inner.len()).unwrap_or(0));
        println!("Active uniques: {}", self.active_uniques.as_ref().map(|inner| inner.len()).unwrap_or(0));
    }

    async fn peers_start_phase2(&self, replica_rcv_msg: Arc<impl Fn(u32, M2) + Send + Sync + 'static>) {
        for (_, peer) in &self.peers {
            peer.start_phase2(replica_rcv_msg.clone()).await;
        }
    }

    async fn peers_start_phase3(&self, replica_rcv_status: Arc<impl Fn(u32, S) + Send + Sync + 'static>) {
        for (_, peer) in &self.peers {
            peer.start_phase3(replica_rcv_status.clone()).await;
        }
    }

    async fn stop_peer_communication(&self) {
        join_all(self.peers.iter().map(|(_, peer)| peer.end_phase())).await;
    }

    async fn phase4(&self) -> bool {
        //check if algo has terminated
        let is_loc_node_active = self.is_active();
        let mut is_hypergraph_active;
        if self.node_id == MASTER_PARTITION {
            is_hypergraph_active = is_loc_node_active;
            for (_, rm) in &self.peers {
                let is_peer_active = rm.rcv_bool().await;
                is_hypergraph_active |= is_peer_active;
            }
            for (_, peer) in &self.peers {
                peer.send_bool(is_hypergraph_active).await;
            }
        } else {
            self.peers.get(&MASTER_PARTITION).unwrap().send_bool(is_loc_node_active).await;
            is_hypergraph_active = self.peers.get(&MASTER_PARTITION).unwrap().rcv_bool().await;
        }
        is_hypergraph_active
    }

    fn update_explicit_tracking_mode<E: Send + Sync + 'static>(explicit_tracking_mode: bool, switch_protection: &mut u8, elements: &HashMap<u32, (AtomicBool, E)>, active_elements: usize) -> bool {
        if *switch_protection > 0 {
            *switch_protection -= 1;
        }
        if *switch_protection == 0 {
            let active_limit = Self::active_limit(elements);
            let should_be_explicit_tracking_mode = active_elements < active_limit;
            if should_be_explicit_tracking_mode != explicit_tracking_mode {
                *switch_protection = 3;
                return true;
            }
        }
        false
    }
    fn active_limit<E: Send + Sync + 'static>(elements: &HashMap<u32, (AtomicBool, E)>) -> usize {
        (elements.len() as f64 * ACTIVE_FACTOR) as usize
    }

    fn is_explicit_tracking_mode<E: Send + Sync + 'static>(elements: &HashMap<u32, (AtomicBool, E)>) -> bool {
        let active_limit = Self::active_limit(&elements);
        elements.par_iter().filter(|(_, (active, _))| active.load(Relaxed)).take_any(active_limit).count() != active_limit
    }

    fn get_ids_active_elements<E: Send + Sync + 'static>(elements: &HashMap<u32, (AtomicBool, E)>) -> DashSet<u32> {
        elements.par_iter().filter_map(|(replica_id, (active, _))|
            if active.load(Relaxed) { Some(*replica_id) } else { None }).collect()
    }
}


async fn create_hypergraph<const LM2: usize, const LS: usize, M1: Send + Sync + 'static, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>, R: Replica<LM2, LS, M1, M2, S>, U: Unique<LM2, M1, M2>>(node_addresses_filepath: &str, node_id: u8, combine: Option<fn(M2, M2) -> M2>, partition_dir: &str) -> Hypergraph<LM2, LS, M1, M2, S, R, U> {
    let peers = create_peers(node_addresses_filepath, node_id, combine).await;
    let (replicas, uniques, master_replicas) = importer::import::<LM2, LS, M1, M2, S, R, U>(partition_dir);
    Hypergraph::new(replicas, uniques, master_replicas, peers, node_id)
}

pub async fn run<const LM2: usize, const LS: usize, M1: Send + Sync + 'static, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>, R: Replica<LM2, LS, M1, M2, S>, U: Unique<LM2, M1, M2>>(partition_dir: &str, node_addresses_filepath: &str, node_id: u8, superstep_limit: Option<u16>, combine: Option<fn(M2, M2) -> M2>) {
    let hypergraph = Arc::new(create_hypergraph::<LM2, LS, M1, M2, S, R, U>(node_addresses_filepath, node_id, combine, partition_dir).await);
    hypergraph.run(superstep_limit).await;
}