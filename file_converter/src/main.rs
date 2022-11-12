use tokio;
use tokio::fs;
use tokio::io::{self, AsyncReadExt};

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut entries = fs::read_dir(".").await?;

    while let Some(entry) = entries.next_entry().await? {
        println!("{:?}", entry.path());
    }
    Ok(())

}