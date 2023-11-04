use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Lines, Write};
use std::iter::Skip;
use std::sync::Arc;

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

pub const MASTER_RMS_FILENAME: &str = "master_partitions.bin";
pub const META_INFO_FILENAME: &str = "meta.yaml";
pub const REPLICAS_FILENAME: &str = "replicas.bin";
pub const UNIQUES_FILENAME: &str = "uniques.bin";
pub const REPLICA_WEIGHS_FILE: &str = "replica_weighs.csv";
pub const UNIQUE_WEIGHS_FILE: &str = "unique_weighs.csv";
pub const UNIQUES_AS_HYPEREDGES_YAML_KEY: &str = "uniques_as_hyperedges";
//n gives the total number of hypervertices
pub const TOTAL_REPLICA_ELEMENTS_YAML_KEY: &str = "total_replica_elements";
//m gives the total number of hyperedges
pub const TOTAL_UNIQUE_ELEMENTS_YAML_KEY: &str = "total_unique_elements";

pub fn partition(hypergraph_filepath: &str, out_dir: &str, total_partitions: u8, uniques_as_hyperedges: bool, unique_partitions_dir: Option<&str>) {
    println!("Write partitions to {out_dir:?}...");
    let get_partition_id = create_get_partition_id(unique_partitions_dir, total_partitions);

    let mut lines_iter = get_lines_iter_with_offset(hypergraph_filepath, 0);
    let has_weigh = match lines_iter.next().unwrap().unwrap().trim() {
        "AdjacencyHypergraph" => false,
        "WeightedAdjacencyHypergraph" => true,
        _ => panic!("First line of input file must be 'AdjacencyHypergraph' or 'WeightedAdjacencyHypergraph'!")
    };

    //v stands for hypervertices
    let total_vs = next_as_u32(&mut lines_iter).unwrap();
    let total_v_neighbors = next_as_u32(&mut lines_iter).unwrap();
    //e stands for hyperedges
    let total_es = next_as_u32(&mut lines_iter).unwrap();
    let total_e_neighbors = next_as_u32(&mut lines_iter).unwrap();

    let total_v_out_weighs = if has_weigh { total_v_neighbors } else { 0 };

    let v_offset_begin = 5;
    let e_offset_begin = v_offset_begin + total_vs + total_v_neighbors + total_v_out_weighs;

    let (total_replicas, total_replica_neighbors, replica_offset_begin, total_uniques, total_unique_neighbors, unique_offset_begin) = if uniques_as_hyperedges {
        (total_vs, total_v_neighbors, v_offset_begin, total_es, total_e_neighbors, e_offset_begin)
    } else {
        (total_es, total_e_neighbors, e_offset_begin, total_vs, total_v_neighbors, v_offset_begin)
    };
    for partition in 0..total_partitions { 
        let dir = format!("{out_dir}/partition-{partition}");
        fs::create_dir_all(dir).unwrap();
    }

    let old_replica_id_to_weigh = if has_weigh {
        Some(import_weigh_strings(hypergraph_filepath, unique_offset_begin + total_uniques, total_unique_neighbors))
    } else { None };
    let old_to_new_replica_id = partition_and_write_replicas(hypergraph_filepath, out_dir, &get_partition_id, total_partitions, total_replicas, replica_offset_begin, total_replica_neighbors, old_replica_id_to_weigh);

    let unique_id_to_weigh = if has_weigh {
        Some(import_weigh_strings(hypergraph_filepath, replica_offset_begin + total_replicas, total_replica_neighbors))
    } else { None };
    partition_and_write_uniques(hypergraph_filepath, out_dir, get_partition_id, total_uniques, unique_offset_begin, total_unique_neighbors, &old_to_new_replica_id, total_partitions, unique_id_to_weigh);
    write_meta_info(out_dir, uniques_as_hyperedges, total_vs, total_es, total_partitions);
}

fn import_weigh_strings(hypergraph_filepath: &str, adversary_element_neighbors_begin: u32, total_adversary_element_neighbors: u32) -> HashMap<u32, String> {
    let id_lines = get_lines_iter_with_offset(hypergraph_filepath, adversary_element_neighbors_begin as usize);
    let weigh_lines = get_lines_iter_with_offset(hypergraph_filepath, adversary_element_neighbors_begin as usize + total_adversary_element_neighbors as usize);
    let id_to_weigh_iterator = id_lines.zip(weigh_lines);
    id_to_weigh_iterator.take(total_adversary_element_neighbors as usize).map(|(id_line_result, weigh_line_result)| (id_line_result.unwrap().parse::<u32>().unwrap(), weigh_line_result.unwrap())).collect()
}

fn partition_and_write_replicas(hypergraph_filepath: &str, out_dir: &str, get_partition_id: &Arc<dyn Fn(u32) -> u8 + Sync + Send + 'static>, total_partitions: u8, total_replicas: u32, replica_offset: u32, total_replica_neighbors: u32, old_id_to_weigh: Option<HashMap<u32, String>>) -> HashMap<u32, u32> {
    let mut partition_to_replicas_writers = create_partition_to_writer(out_dir, REPLICAS_FILENAME, total_partitions);
    let mut partition_to_master_replicas_writers = create_partition_to_writer(out_dir, MASTER_RMS_FILENAME, total_partitions);
    let mut partition_to_weigh_writers = old_id_to_weigh.as_ref().map(|_| create_partition_to_writer(out_dir, REPLICA_WEIGHS_FILE, total_partitions));

    let mut lines_iter = get_lines_iter_with_offset(hypergraph_filepath, replica_offset as usize);

    let replica_neighborhood_ends = import_neighborhood_ends(total_replicas, total_replica_neighbors, &mut lines_iter);

    let mut partition_to_highest_unused_replica_id: HashMap<u8, u32> = HashMap::new();
    let mut old_to_new_replica_id = HashMap::with_capacity(total_replicas as usize);
    let mut neighborhood_begin = 0;
    let mut rng = StdRng::seed_from_u64(7593741937);

    for (old_id, neighborhood_end) in replica_neighborhood_ends.into_iter().enumerate() {
        let partition_to_neighbored_uniques = import_partition_to_neighbored_uniques(&mut lines_iter, neighborhood_begin, neighborhood_end, get_partition_id, total_partitions);

        let master_partition = assign_master_partition(&partition_to_neighbored_uniques, &mut rng);

        let new_id = assign_new_id(total_partitions, &mut partition_to_highest_unused_replica_id, master_partition);
        let old_id = old_id as u32;
        old_to_new_replica_id.insert(old_id, new_id);

        write_master_replica_to_partitions(&mut partition_to_master_replicas_writers, &partition_to_neighbored_uniques, master_partition, new_id);

        let total_neighborhood = neighborhood_end - neighborhood_begin;
        write_replica(&mut partition_to_replicas_writers, &partition_to_neighbored_uniques, new_id, old_id, total_neighborhood);

        //write weigh
        if let Some(replica_old_id_to_weigh) = old_id_to_weigh.as_ref() {
            if let Some(partition_to_weigh_writers) = partition_to_weigh_writers.as_mut() {
                let line = format!("{}, {}\n", new_id, replica_old_id_to_weigh.get(&old_id).unwrap());
                for (partition, _) in &partition_to_neighbored_uniques {
                    partition_to_weigh_writers.get_mut(partition).unwrap().write(line.as_bytes()).unwrap();
                }
            }
        }
        neighborhood_begin = neighborhood_end;
    }
    flush_all(&mut partition_to_replicas_writers);
    flush_all(&mut partition_to_master_replicas_writers);
    if let Some(partition_to_weigh_writers) = partition_to_weigh_writers.as_mut() {
        flush_all(partition_to_weigh_writers);
    }
    old_to_new_replica_id
}

fn import_neighborhood_ends(total_vs: u32, total_neighbors: u32, mut lines_iter: &mut Skip<Lines<BufReader<File>>>) -> Vec<u32> {
    //offsets always give the beginning of a neighborhood -> the beginning of the next neighborhood is the end of the current one
    //first offset is not the end of a neighborhood
    lines_iter.next();
    let mut replica_neighborhood_ends: Vec<_> = (0..(total_vs - 1)).map(|_| next_as_u32(&mut lines_iter).unwrap()).collect();
    //the end of the last neighborhood is the total number of neighbors
    replica_neighborhood_ends.push(total_neighbors);
    replica_neighborhood_ends
}

fn create_partition_to_writer(out_dir: &str, filename: &str, total_partitions: u8) -> HashMap<u8, BufWriter<File>> {
    (0..total_partitions).into_iter().map(|partition| {
        let filename = format!("{out_dir}/partition-{partition}/{filename}");
        let file = File::create(filename).unwrap();
        let writer = BufWriter::new(file);
        (partition, writer)
    }).collect()
}

fn import_partition_to_neighbored_uniques(lines_iter: &mut Skip<Lines<BufReader<File>>>, neighborhood_begin: u32, neighborhood_end: u32, get_partition_id: &Arc<dyn Fn(u32) -> u8 + Sync + Send>, total_partitions: u8) -> HashMap<u8, Vec<u32>> {
    let mut partition_to_neighbors = HashMap::new();
    if neighborhood_begin == neighborhood_end {
        //replica has no outgoing edges -> assign to random partition
        let mut rng = StdRng::seed_from_u64(neighborhood_begin as u64);
        let rand: u8 = rng.gen();
        let partition = rand % total_partitions;
        partition_to_neighbors.insert(partition, Vec::new());
    } else {
        (neighborhood_begin..neighborhood_end)
            .map(|_| { next_as_u32(lines_iter).unwrap() })
            .for_each(|unique_id| {
                let partition = (*get_partition_id)(unique_id);
                let current_neighbors = partition_to_neighbors.entry(partition).or_insert(Vec::new());
                current_neighbors.push(unique_id);
            });
    }
    partition_to_neighbors
}

fn flush_all(partition_to_writers: &mut HashMap<u8, BufWriter<File>>) {
    for (_, writer) in partition_to_writers {
        writer.flush().unwrap();
    }
}


fn write_replica(partition_to_replica_writer: &mut HashMap<u8, BufWriter<File>>, partition_to_replica_loc_neighbors: &HashMap<u8, Vec<u32>>, new_id: u32, old_id: u32, total_neighbors: u32) {
    // Add local replicas
    for (partition, loc_neighbors) in partition_to_replica_loc_neighbors {
        let replica_writer = partition_to_replica_writer.get_mut(&partition).unwrap();
        replica_writer.write(&old_id.to_be_bytes()).unwrap();
        replica_writer.write(&new_id.to_be_bytes()).unwrap();

        replica_writer.write(&total_neighbors.to_be_bytes()).unwrap();
        let loc_neighbors_len = loc_neighbors.len() as u32;
        replica_writer.write(&loc_neighbors_len.to_be_bytes()).unwrap();
        for neighbor in loc_neighbors {
            replica_writer.write(&neighbor.to_be_bytes()).unwrap();
        }
    }
}

fn write_master_replica_to_partitions(partition_to_master_replicas_writers: &mut HashMap<u8, BufWriter<File>>, partitions_to_neighbors: &HashMap<u8, Vec<u32>>, master_partition: u8, replica_id: u32) {
    //store to which partitions a master replica has to send its value
    let master_partitions: Vec<_> = partitions_to_neighbors.iter()
        .filter(|(partition, _)| **partition != master_partition)
        .map(|(peer, _)| *peer)
        .collect();
    if !master_partitions.is_empty() {
        let writer = partition_to_master_replicas_writers.get_mut(&master_partition).unwrap();
        writer.write(&replica_id.to_be_bytes()).unwrap();
        let total_partitions = master_partitions.len() as u8;
        writer.write(&total_partitions.to_be_bytes()).unwrap();
        for partition in master_partitions {
            writer.write(&partition.to_be_bytes()).unwrap();
        }
    }
}

fn assign_new_id(total_partitions: u8, partition_to_highest_unused_replica_id: &mut HashMap<u8, u32>, master_partition: u8) -> u32 {
    //get the highest free id that modulos to master_replica_partition_id
    let new_replica_id = *partition_to_highest_unused_replica_id.entry(master_partition).and_modify(|i| *i = i.checked_add(total_partitions as u32).unwrap()).or_insert(master_partition as u32);
    if new_replica_id == u32::MAX {
        panic!("{} encodes the end of a message and can therefore not be an id!", u32::MAX);
    }
    new_replica_id
}

fn assign_master_partition(partitions_to_neighbors: &HashMap<u8, Vec<u32>>, rng: &mut StdRng) -> u8 {
    //make a random partition to the master partition
    let rand: u8 = rng.gen();
    let n = rand % partitions_to_neighbors.len() as u8;
    let mut counter: u8 = 0;
    let (master_partition, _) = partitions_to_neighbors.iter()
        .find(|_| {
            if counter == n {
                true
            } else {
                counter += 1;
                false
            }
        }).unwrap();
    *master_partition
}

fn partition_and_write_uniques(filepath: &str, out_dir: &str, get_partition_id: Arc<dyn Fn(u32) -> u8 + Sync + Send + 'static>, total_uniques: u32, unique_offset_begin: u32, total_unique_neighbors: u32, old_to_new_replica_id: &HashMap<u32, u32>, total_partitions: u8, unique_id_to_weigh: Option<HashMap<u32, String>>) {
    let mut partition_to_uniques_writer = create_partition_to_writer(out_dir, UNIQUES_FILENAME, total_partitions);
    let mut partition_to_weigh_writer = unique_id_to_weigh.as_ref().map(|_| create_partition_to_writer(out_dir, UNIQUE_WEIGHS_FILE, total_partitions));

    let mut lines_iter = get_lines_iter_with_offset(filepath, unique_offset_begin as usize);

    let unique_neighborhood_ends = import_neighborhood_ends(total_uniques, total_unique_neighbors, &mut lines_iter);

    let mut neighborhood_begin = 0;
    for (unique_id, neighborhood_end) in unique_neighborhood_ends.into_iter().enumerate() {
        let unique_id = unique_id as u32;
        let partition = (*get_partition_id)(unique_id);
        let writer = partition_to_uniques_writer.get_mut(&partition).unwrap();
        writer.write(&unique_id.to_be_bytes()).unwrap();
        let total_neighbors = neighborhood_end - neighborhood_begin;
        writer.write(&total_neighbors.to_be_bytes()).unwrap();
        for _ in 0..total_neighbors {
            let old_replica_id = next_as_u32(&mut lines_iter).unwrap();
            let new_replica_id = *old_to_new_replica_id.get(&old_replica_id).unwrap();
            writer.write(&new_replica_id.to_be_bytes()).unwrap();
        }
        if let Some(node_to_weigh_writer) = partition_to_weigh_writer.as_mut() {
            let weigh_writer = node_to_weigh_writer.get_mut(&partition).unwrap();
            let line = format!("{unique_id}, {}\n", unique_id_to_weigh.as_ref().unwrap().get(&unique_id).unwrap());
            weigh_writer.write(line.as_bytes()).unwrap();
        }
        neighborhood_begin = neighborhood_end;
    }

    flush_all(&mut partition_to_uniques_writer);
    if let Some(node_to_weigh_writer) = partition_to_weigh_writer.as_mut() {
        flush_all(node_to_weigh_writer);
    }
}

fn write_meta_info(base_dir: &str, uniques_as_hyperedeges: bool, hypervertices: u32, hyperedges: u32, total_partitions: u8) {
    for partition_id in 0..total_partitions {
        let filepath = format!("{}/partition-{partition_id}/{META_INFO_FILENAME}", base_dir);
        let mut file = File::create(filepath).unwrap();
        let (total_replica_elements, total_unique_elements) = if uniques_as_hyperedeges {
            (hypervertices, hyperedges)
        } else {
            (hyperedges, hypervertices)
        };

        let content = format!("{UNIQUES_AS_HYPEREDGES_YAML_KEY}: {uniques_as_hyperedeges:?}\n\
            {TOTAL_REPLICA_ELEMENTS_YAML_KEY}: {total_replica_elements:?}\n\
            {TOTAL_UNIQUE_ELEMENTS_YAML_KEY}: {total_unique_elements:?}\n");
        file.write_all(&content.into_bytes()).unwrap();
    }
}

fn get_lines_iter_with_offset(filepath: &str, offset: usize) -> Skip<Lines<BufReader<File>>> {
    let file = File::open(filepath).expect(&*format!("Can't open file at {filepath:?}"));
    let reader = BufReader::new(file);
    let lines_iter = reader.lines();
    lines_iter.skip(offset)
}

fn next_as_u32(lines_iter: &mut Skip<Lines<BufReader<File>>>) -> Option<u32> {
    let string = lines_iter.next()?.unwrap();
    Some(string.parse().expect(&*format!("not an unsigned int: '{string}'")))
}

fn create_get_partition_id(partition_dir: Option<&str>, total_partitions: u8) -> Arc<dyn Fn(u32) -> u8 + Sync + Send + 'static> {
    match partition_dir {
        // Read the directory contents
        Some(dir_str) => {
            let dir = fs::read_dir(dir_str).unwrap();
            let mut unique_id_to_partition = HashMap::new();
            for entry_result in dir {
                if let Ok(entry) = entry_result {
                    if entry.file_type().unwrap().is_file() {
                        let filename = entry.file_name().into_string().unwrap();
                        if filename.starts_with("partition-") {
                            let partition_id_str = &filename[10..];
                            let partition_id = partition_id_str.parse::<u8>().unwrap();
                            let mut lines_iter = get_lines_iter_with_offset((dir_str.to_owned() + "/" + filename.as_str()).as_str(), 0);
                            while let Some(unique_id) = next_as_u32(&mut lines_iter) {
                                unique_id_to_partition.insert(unique_id, partition_id);
                            }
                        }
                    }
                }
            }
            Arc::new(move |unique_id| *unique_id_to_partition.get(&unique_id).unwrap())
        }
        None => Arc::new(move |unique_id| ((unique_id % (total_partitions as u32)) as u8))
    }
}