# Dyper - A Framework for Processing Hypergraphs on a Distributed System

## Introdcution

This repository contains the code for Dyper, a distributed hypergraph processing system
which is distributed and fast. It allows to implement hypergraph algorithms in a
message based way, where hypervertices and hyperedges send messages to each
other. The user only has to define a few local methods on the hypervertices and
hyperedges. The rest of the algorithm, like delivering messages, is then taken care of
by the framework.
Our experiments on up to 8 2-core nodes showed that Dyper is 4 to 14 times faster than
other distributed frameworks.
Dyper was the topic of my Master's thesis, which can be downloaded [here](resources/Dyper%20-%20A%20Framework%20for%20Processing%20Hypergraphs%20on%20a%20Distributed%20System.pdf).

## Installation
Tested on Ubuntu 20.04 LTS.
```bash
    # install rustup. Choose "1) Proceed with installation (default)".
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    source "$HOME/.cargo/env"
    
    #install cc linker for Rust compiler
    sudo apt update
    sudo apt install build-essential
    
    # clone the repository
    git clone https://github.com/lukasdenk/dyper.git
    
    # build and start the project
    cd dyper
    ~/.cargo/bin/cargo run --release -- --help
```

## Usage

Running an algorithm requires two steps:

1. Partitioning the hypergraph. This is done with the `partition` command. For more information
   run `cargo run --release -- partition --help`. This prints the following:

```
Usage: dyper partition [OPTIONS] <DIR> <PARTITIONS>

Arguments:
  <DIR>         Specifies the path to the hypergraph input file or directory. Suspects the format to be as described here: https://github.com/jshun/ppopp20-ae#input-format-for-hygra-applications
  <PARTITIONS>  Specifies the number of partitions to create

Options:
  -o, --out <DIR>             Specifies the path to the directory where the partitions will be written to
  -p, --partitioning <DIR>    Specifies the path to the directory containing the partitioning of the uniques. For every partition i, the directory must have a file partition-<i>.txt which contains the ids of the uniques that should be assigned to that partition. Each line should contain one id. If not specified, the uniques will be partitioned randomly
      --partition-hyperedges  Specifies that the partitioning is by the set of hyperedges. In this case, the uniques represent hyperedges and the replicas represent vertices
  -h, --help                  Print help
  -V, --version               Print version
```

2. Running each nodes with the `nodes` command. For more information run `cargo run --release -- nodes --help`. This
   prints the following:

```
Usage: dyper node [OPTIONS] <ALGORITHM> <NODE_ID> <PARTITION> <ADDRESSES>

Arguments:
  <ALGORITHM>  Specifies the algorithm to run [possible values: page-rank, shortest-path, connected-component, label-propagation]
  <NODE_ID>    Specifies the partition number of the node, starting from 0
  <PARTITION>  Specifies the path to the directory containing the partition
  <ADDRESSES>  Specifies the path to the file containing the addresses of the nodes. Line i contains the address of node i in the format <ip>:<port>

Options:
  -s, --superstep-limit <SUPERSTEP_LIMIT>
          
      --source <SOURCE>
          Specifies the source hypervertex for algorithms that start at a source hypervertex. If not specified, this results to 0
  -h, --help
          Print help
  -V, --version
          Print version

```

## Implementing Further Algorithms

To implement further algorithms, check out how this is done for the existing algorithms in `src/algos`.

