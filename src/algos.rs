use std::fmt::Debug;

use clap::ValueEnum;

use crate::hypergraph::run;
use crate::partitioner;

pub mod page_rank;
pub mod connected_components;
mod shortest_path;
mod label_propagation;

pub static mut UNIQUES_AS_HYPEREDGES: bool = false;
//Total number of hypervertices (hyperedges) if UNIQUES_AS_HYPEREDGES is true (false). In the master thesis, this was called "n".
pub static mut TOTAL_REPLICA_ELEMENTS: u32 = 0;
//Total number of hyperedges (hypervertices) if UNIQUES_AS_HYPEREDGES is true (false). In the master thesis, this was called "m".
pub static mut TOTAL_UNIQUE_ELEMENTS: u32 = 0;
pub static mut SOURCE_HYPERVERTEX: u32 = 0;


#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Algorithm {
    PageRank,
    ShortestPath,
    ConnectedComponent,
    LabelPropagation
}

pub trait Replica<const LM2: usize, const LS: usize, M1: Send + Sync + 'static, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>>: Sync + Send + 'static + Sized {
    fn new(old_id: u32, id: u32, local_out_neighbors: Vec<u32>, total_out_neighborhood_size: u32, weigh: Option<&str>) -> (Self, bool);
    fn update(&self, send_msg: impl Fn(u32, M1), superstep: u16) -> bool;
    //returns if the replica should be activated
    fn rcv_msg(&self, msg: M2) -> bool;
    fn get_status(&self) -> S;
    fn rcv_status(&self, status: S);
}

pub trait Unique<const LM2: usize, M1: Send + Sync + 'static, M2: ToFromBytes<LM2>>: Sync + Send + 'static + Sized {
    fn new(id: u32, out_neighbors: Vec<u32>, weigh: Option<&str>) -> (Self, bool);
    fn update(&self, send_msg: impl Fn(u32, M2), superstep: u16) -> bool;
    //returns if the unique should be activated
    fn rcv_msg(&self, msg: M1) -> bool;
}

pub trait ToFromBytes<const N: usize>: Sync + Send + Copy + 'static + Unpin + Debug {
    fn to_bytes(&self) -> [u8; N];
    fn from_bytes(bytes: [u8; N]) -> Self;
}

impl ToFromBytes<4> for u32 {
    fn to_bytes(&self) -> [u8; 4] {
        self.to_le_bytes()
    }

    fn from_bytes(bytes: [u8; 4]) -> Self {
        u32::from_le_bytes(bytes)
    }
}

impl ToFromBytes<8> for f64 {
    fn to_bytes(&self) -> [u8; 8] {
        self.to_le_bytes()
    }

    fn from_bytes(bytes: [u8; 8]) -> Self {
        f64::from_le_bytes(bytes)
    }
}

pub async fn start(algo: Algorithm, partition_dir: &str, node_addresses_filepath: &str, node_id: u8, superstep_limit: Option<u16>, source_hypervertex: Option<u32>) {
    init_glob_vars(partition_dir, source_hypervertex);
    match algo {
        Algorithm::PageRank => run::<8, 8, f64, f64, f64, page_rank::PageRankReplica, page_rank::PageRankUnique>(partition_dir, node_addresses_filepath, node_id, superstep_limit, Some(page_rank::COMBINE)).await,
        Algorithm::ConnectedComponent => run::<4, 4, u32, u32, u32, connected_components::CCV1, connected_components::CCV2>(partition_dir, node_addresses_filepath, node_id, superstep_limit, Some(connected_components::COMMUTE)).await,
        Algorithm::ShortestPath => {
            run::<4, 4, u32, u32, u32, shortest_path::SPV1, shortest_path::SPV2, >(partition_dir, node_addresses_filepath, node_id, superstep_limit, Some(shortest_path::COMMUTE)).await
        },
        Algorithm::LabelPropagation => run::<4, 4, u32, u32, u32, label_propagation::LpReplica, label_propagation::LpUnique>(partition_dir, node_addresses_filepath, node_id, superstep_limit, None).await
    };
}

fn init_glob_vars(partition_dir: &str, source_hypervertex: Option<u32>) {
    let meta_info_filepath = format!("{partition_dir}/{}", partitioner::META_INFO_FILENAME);
    let string = std::fs::read_to_string(meta_info_filepath.clone()).expect(format!("Can't find file {meta_info_filepath}").as_str());
    for line in string.lines() {
        let mut splitted = line.split(":");
        let key = splitted.next().unwrap().trim();
        let value = splitted.next().unwrap().trim();
        match key.to_lowercase().as_str() {
            partitioner::UNIQUES_AS_HYPEREDGES_YAML_KEY => {
                let uniques_as_hyperedges = value.to_lowercase() == "uniques_as_hyperedges";
                unsafe { UNIQUES_AS_HYPEREDGES = uniques_as_hyperedges };
            }
            partitioner::TOTAL_REPLICA_ELEMENTS_YAML_KEY => {
                let total_replica_elements = value.parse().unwrap();
                unsafe { TOTAL_REPLICA_ELEMENTS = total_replica_elements };
            }
            partitioner::TOTAL_UNIQUE_ELEMENTS_YAML_KEY => {
                let total_unique_elements = value.parse().unwrap();
                unsafe { TOTAL_UNIQUE_ELEMENTS = total_unique_elements };
            }
            _ => panic!("Parsing {meta_info_filepath} failed. Unknown key {key}.")
        }
    }

    if let Some(source_hypervertex) = source_hypervertex {
        unsafe { SOURCE_HYPERVERTEX = source_hypervertex };
    }
}
