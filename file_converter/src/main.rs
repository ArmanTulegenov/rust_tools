use std::fs::File;
use std::io::{Read, BufReader};
use std::path::PathBuf;

use flate2::read::GzDecoder;
use tokio;
use tokio::fs;
use tokio::io;
use itertools::Itertools;
use futures::stream::FuturesUnordered;
use serde_json::{Result, Value};


#[tokio::main]
async fn main() -> io::Result<()> {

    let mut q: Vec<PathBuf> = vec![];

    let dir_path = std::env::args().nth(1).expect("Please provide a directory as command line argument.");
    let pattern = std::env::args().nth(2).expect("Please provide a pattern as command line argument.");

    let mut entries = fs::read_dir(&dir_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name().to_string_lossy().to_string();
        if file_name.contains(&pattern) {
            q.push(entry.path());
        }
    }

    dispatch(q).await;

    Ok(())
    
}

async fn dispatch(list_of_files: Vec<PathBuf>) -> io::Result<()> {

    let mut files = list_of_files.into_iter().tuples();

    for (prev, next) in files.by_ref() {

        let paths = vec![prev, next].into_iter();    

        let tasks = paths.map(|path| tokio::spawn(
            try_to_extract_and_process(path))).collect::<FuturesUnordered<_>>();
            
        futures::future::join_all(tasks).await;
    }
    for leftover in files.into_buffer() {
        try_to_extract_and_process(leftover).await;
    }    
    Ok(())
}

async fn try_to_extract_and_process(entry: PathBuf) -> io::Result<()> {
    if let Some(e) = entry.extension() { 
        let file_extension = e.to_string_lossy();
        if file_extension.to_string() == "gz" {
            println!("Start at {:?}: {:?}", chrono::offset::Local::now(), entry.as_path());
            let mut d = GzDecoder::new(std::fs::File::open(&entry).unwrap());
            let mut s = String::new();
            d.read_to_string(&mut s)?;
            process_text_lines(s.as_str()).await;
            println!("Done at {:?}: {:?}", chrono::offset::Local::now(), entry.as_path());
        } else {
            let file = File::open("foo.txt")?;
            let mut reader = BufReader::new(file);
            let mut s = String::new();
            reader.read_to_string(&mut s)?;
            process_text_lines(s.as_str()).await;
            println!("{:?}", entry);
        }
    }
    Ok(())
}

async fn process_text_lines(lines: &str ) -> io::Result<()> {
    
    for line in lines.lines() {
        let v: Value = serde_json::from_str(line)?;
        println!("{:?}", v);
    }

    Ok(())
}