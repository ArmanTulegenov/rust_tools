use tokio;
use tokio::fs;
use tokio::io::{self, AsyncReadExt};

#[tokio::main]
async fn main() -> io::Result<()> {

    let dir_path = std::env::args().nth(1).expect("Please provide a directory as command line argument.");
    let pattern = std::env::args().nth(2).expect("Please provide a pattern as command line argument.");

    let mut entries = fs::read_dir(&dir_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_name().to_string_lossy().contains(&pattern) {
            println!("{:?}", entry.path());
        }
    }
    Ok(())

    
}