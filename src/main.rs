use clap::{arg, command, Parser, Subcommand};

use crate::algos::Algorithm;

mod hypergraph;
mod algos;
mod importer;
mod peer;
mod partitioner;


#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.take_command() {
        Commands::Node {
            algorithm,
            node_id: partition_nr,
            partition: partition_dir,
            superstep_limit: rounds,
            addresses,
            source: source_vertex
        } => {
            algos::start(
                algorithm,
                partition_dir.as_str(),
                addresses.as_str(),
                partition_nr,
                rounds,
                source_vertex,
            ).await;
        }
        Commands::Partition {
            hypergraph,
            partitions,
            out: out_dir,
            partitioning: unique_partitioning,
            partition_hyperedges: uniques_as_vertices
        } => {
            let default_out_dir = ".".to_string();
            let out_dir = out_dir.unwrap_or(default_out_dir);
            partitioner::partition(
                hypergraph.as_str(),
                out_dir.as_str(),
                partitions,
                !uniques_as_vertices,
                unique_partitioning.as_ref().map(std::string::String::as_str),
            )
        }
    }
}


#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

impl Cli {
    pub fn take_command(self) -> Commands {
        self.command
    }
}

#[derive(Subcommand)]
pub enum Commands {
    Partition {
        /// Specifies the path to the hypergraph input file or directory.
        /// Suspects the format to be as described here: https://github.com/jshun/ppopp20-ae#input-format-for-hygra-applications
        #[arg(value_name = "DIR")]
        hypergraph: String,
        /// Specifies the number of partitions to create.
        partitions: u8,
        #[arg(long, value_name = "DIR")]
        /// Specifies the path to the directory where the partitions will be written to.
        #[arg(short, long, value_name = "DIR")]
        out: Option<String>,
        /// Specifies the path to the directory containing the partitioning of the uniques. For every partition i, the directory must have a file partition-<i>.txt which
        /// contains the ids of the uniques that should be assigned to that partition. Each line should contain one id. If not specified, the uniques will be partitioned randomly.
        #[arg(short, long, value_name = "DIR")]
        partitioning: Option<String>,
        /// Specifies that the partitioning is by the set of hyperedges. In this case, the uniques represent hyperedges and the replicas represent vertices.
        #[arg(long)]
        partition_hyperedges: bool,
    },
    Node {
        /// Specifies the algorithm to run.
        algorithm: Algorithm,
        /// Specifies the partition number of the node, starting from 0.
        node_id: u8,
        /// Specifies the path to the directory containing the partition.
        partition: String,
        /// Specifies the path to the file containing the addresses of the nodes. Line i contains the address of node i in the format <ip>:<port>.
        addresses: String,
        #[arg(short, long)]
        superstep_limit: Option<u16>,
        /// Specifies the source hypervertex for algorithms that start at a source hypervertex. If not specified, this results to 0.
        #[arg(long)]
        source: Option<u32>,
    },
}