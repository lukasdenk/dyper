use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::atomic::AtomicBool;

use crate::algos::{Replica, ToFromBytes, Unique};
use crate::partitioner;

pub fn import<const LM2: usize, const LS: usize, M1: Send + Sync + 'static, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>, R: Replica<LM2, LS, M1, M2, S>, U: Unique<LM2, M1, M2>>(partition_dir: &str) -> (HashMap<u32, (AtomicBool, R)>, HashMap<u32, (AtomicBool, U)>, HashMap<u32, Vec<u8>>) {
    let master_replicas = import_master_replicas(partition_dir);

    let replica_weighs_filepath = format!("{}/{}", partition_dir, partitioner::REPLICA_WEIGHS_FILE);
    let replica_to_weigh = if Path::new(replica_weighs_filepath.as_str()).exists() {
        Some(import_weighs(replica_weighs_filepath.as_str()))
    } else { None };

    let replicas_filepath = format!("{}/{}", partition_dir, partitioner::REPLICAS_FILENAME);
    let replicas = import_replicas(replicas_filepath.as_str(), replica_to_weigh);

    let unique_weighs_filepath = format!("{}/{}", partition_dir, partitioner::UNIQUE_WEIGHS_FILE);
    let unique_to_weigh = if Path::new(unique_weighs_filepath.as_str()).exists() {
        Some(import_weighs(unique_weighs_filepath.as_str()))
    } else { None };

    let uniques_filepath = format!("{}/{}", partition_dir, partitioner::UNIQUES_FILENAME);
    let uniques = import_uniques(uniques_filepath.as_str(), unique_to_weigh);

    (replicas, uniques, master_replicas)
}

fn import_weighs(filepath: &str) -> HashMap<u32, String> {
    let file = File::open(filepath).unwrap();
    let reader = BufReader::new(file);

    // Create a HashMap to store the key-value pairs
    let mut id_to_weigh: HashMap<u32, String> = HashMap::new();

    // Iterate over the lines in the CSV file
    for line_result in reader.lines() {
        let line = line_result.unwrap();
        let mut parts = line.split(',');

        // Parse the key (u32) and value (String)
        let id_str = parts.next().unwrap().trim();
        let weigh = parts.next().unwrap().trim();
        let id = id_str.parse::<u32>().unwrap();

        id_to_weigh.insert(id, weigh.to_string());
    }
    id_to_weigh
}

fn import_replicas<const LM2: usize, const LS: usize, M1: Send + Sync + 'static, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>, R: Replica<LM2, LS, M1, M2, S>>(filepath: &str, replica_to_weigh: Option<HashMap<u32, String>>) -> HashMap<u32, (AtomicBool, R)> {
    let file = File::open(filepath).unwrap();
    let mut reader = BufReader::new(file);
    let mut replicas = HashMap::new();

    loop {
        let old_id = match read_u32(&mut reader) {
            Ok(inner) => inner,
            //EOF
            Err(_) => break
        };
        let id = read_u32(&mut reader).unwrap();

        let total_out_neighborhood_size = read_u32(&mut reader).unwrap();
        let loc_neighbors_len = read_u32(&mut reader).unwrap() as usize;
        let local_out_neighbors = (0..loc_neighbors_len).into_iter().map(|_| {
            read_u32(&mut reader).unwrap()
        }).collect();

        let weigh = replica_to_weigh.as_ref().map(|inner| inner.get(&id).unwrap().as_str());
        let (replica, active) = R::new(old_id, id, local_out_neighbors, total_out_neighborhood_size, weigh);
        replicas.insert(id, (AtomicBool::new(active), replica));
    }
    replicas
}

fn import_uniques<const N: usize, M1: Send + Sync + 'static, M2: ToFromBytes<N>, U: Unique<N, M1, M2>>(filepath: &str, unique_to_weigh: Option<HashMap<u32, String>>) -> HashMap<u32, (AtomicBool, U)> {
    let file = File::open(filepath).unwrap();
    let mut reader = BufReader::new(file);
    let mut uniques = HashMap::new();

    loop {
        let id = match read_u32(&mut reader) {
            Ok(inner) => inner,
            //EOF
            Err(_) => break
        };

        let neighbors_len = read_u32(&mut reader).unwrap() as usize;
        let out_neighbors = (0..neighbors_len).into_iter().map(|_| {
            read_u32(&mut reader).unwrap()
        }).collect();

        let weigh = unique_to_weigh.as_ref().map(|inner| inner.get(&id).unwrap().as_str());
        let (unique, active) = U::new(id, out_neighbors, weigh);
        uniques.insert(id, (AtomicBool::new(active), unique));
    }
    uniques
}

fn read_u32(reader: &mut BufReader<File>) -> Result<u32, std::io::Error> {
    let mut u32_bytes = [0u8; 4];
    reader.read_exact(&mut u32_bytes)?;
    Ok(u32::from_be_bytes(u32_bytes))
}

fn read_u8(reader: &mut BufReader<File>) -> Result<u8, std::io::Error> {
    let mut u8_bytes = [0u8; 1];
    reader.read_exact(&mut u8_bytes)?;
    Ok(u8::from_be_bytes(u8_bytes))
}

fn import_master_replicas(base_dir: &str) -> HashMap<u32, Vec<u8>> {
    let filepath = format!("{base_dir}/{}", partitioner::MASTER_RMS_FILENAME);
    let file = File::open(&filepath).expect(&*format!("Can't open file at {:?}", &filepath));
    let mut reader = BufReader::new(file);
    let mut master_replicas = HashMap::new();

    loop {
        let replica_id = match read_u32(&mut reader) {
            Ok(val) => val,
            Err(_) => break
        };

        // Read value length
        let rms_len = read_u8(&mut reader).unwrap() as usize;

        // Read rms
        let rms = (0..rms_len).into_iter().map(|_| {
            read_u8(&mut reader).unwrap()
        }).collect();

        master_replicas.insert(replica_id, rms);
    }
    master_replicas
}
