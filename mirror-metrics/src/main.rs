mod nginx;
mod tree;

use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use rayon::prelude::*;
use std::{
    fs::{read_to_string, File},
    io::Read,
    time::Instant,
};

use crate::{
    nginx::{parse_line, LogEntry, Metric},
    tree::Tree,
};

fn process_file(file: &str) -> anyhow::Result<()> {
    // Read file into memory
    let start = Instant::now();
    let file = read_to_string(file).unwrap();
    println!("Read file into memory: {:?}", Instant::now() - start);

    // The first line defines the date of the log file
    let date = file
        .lines()
        .next()
        .map(parse_line)
        .unwrap()
        .unwrap()
        .timestamp
        .date_naive();

    // Parse each line of the log file
    let start = Instant::now();
    let entries = file
        .lines()
        .flat_map(parse_line)
        .filter(|entry| entry.status < 300 && entry.status >= 200 && entry.method == "GET")
        .map(|entry| {
            let LogEntry {
                path,
                bytes_sent,
                bytes_received,
                ..
            } = entry;
            (path, Metric::new(1, bytes_sent, bytes_received))
        })
        .collect::<Vec<_>>();

    println!("Read and parse file: {:?}", Instant::now() - start);

    // Make the path tree
    let start = Instant::now();
    let tree = Tree::from_iter(entries);
    println!("Tree creation time: {:?}", Instant::now() - start);

    // Serialize
    let start = Instant::now();
    // Write to a file depending on the date of the first log entry
    let filename = format!("processed/{}.txt.gz", date);
    let mut writer = GzEncoder::new(File::create(filename).unwrap(), Compression::default());
    tree.serialize(&mut writer).unwrap();
    println!("Serialize time: {:?}", Instant::now() - start);

    Ok(())
}

fn merge(dir: &str) -> anyhow::Result<()> {
    let files = std::fs::read_dir(dir).unwrap().collect::<Vec<_>>();

    let files = files
        .into_par_iter()
        .map(|file| {
            let file = file.unwrap().path();
            file.to_str().unwrap().to_string()
        })
        // Files start gzip compressed, so we need to decompress them
        .map(|file| {
            let file = File::open(file).unwrap();
            let mut decoder = GzDecoder::new(file);
            let mut contents = String::new();
            decoder.read_to_string(&mut contents).unwrap();
            contents
        })
        .collect::<Vec<_>>();

    // Each file gets deserialized into a tree
    let trees = files
        .par_iter()
        .map(|f| Tree::deserialize(f))
        .collect::<anyhow::Result<Vec<_>>>();

    // Merge all trees into one
    let mut tree = Tree::new();
    for t in &trees? {
        tree.union(t);
    }

    // Write to a file
    let filename = "merged.txt.gz".to_string();
    let mut writer = GzEncoder::new(File::create(filename).unwrap(), Compression::default());
    tree.serialize(&mut writer).unwrap();

    Ok(())
}

fn main() {
    // Process all files in logs directory
    let start = Instant::now();
    let files = std::fs::read_dir("logs").unwrap().collect::<Vec<_>>();
    files.into_par_iter().for_each(|file| {
        let file = file.unwrap().path();
        let file = file.to_str().unwrap();
        process_file(file).unwrap();
    });
    println!("Total reduce time: {:?}", Instant::now() - start);

    // Merge all files in processed directory
    let start = Instant::now();
    merge("processed").unwrap();
    println!("Total merge time: {:?}", Instant::now() - start);

    println!("Done!");
}
