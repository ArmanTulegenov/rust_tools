#[macro_use]
extern crate queues;

use std::path::PathBuf;

use queues::{Queue, IsQueue};
use tokio;
use tokio::fs;
use tokio::io::{self, AsyncReadExt};

#[tokio::main]
async fn main() -> io::Result<()> {

    let mut q: Queue<PathBuf> = queue![];

    let dir_path = std::env::args().nth(1).expect("Please provide a directory as command line argument.");
    let pattern = std::env::args().nth(2).expect("Please provide a pattern as command line argument.");

    let mut entries = fs::read_dir(&dir_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name().to_string_lossy().to_string();
        if file_name.contains(&pattern) {
            if let Some(e) = entry.path().extension() { 
                if e.to_string_lossy().to_string() == "gz" {
                    q.add(entry.path());
                    println!("{:?}", entry.path());
                } else {
                    println!("{:?}", entry.path());
                }
            }
        }
    }

    Ok(())
    
}