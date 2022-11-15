use std::fs::File;
use std::io::{Read, BufReader};
use std::path::PathBuf;

use async_recursion::async_recursion;
use flate2::read::GzDecoder;
use tokio;
use tokio::fs;
use tokio::io;
use itertools::Itertools;
use futures::stream::FuturesUnordered;
use serde_json::{Result, Value};
use clap::{arg, command, value_parser, ArgAction, Command};

#[tokio::main]
async fn main() -> io::Result<()> {

    let matches = command!() // requires `cargo` feature
        .arg(
            arg!(
                -d --directory <DIRECTORY> "A path to directory with json files."
            )
            // We don't have syntax yet for optional options, so manually calling `required`
            .value_parser(value_parser!(PathBuf)),
        )
        .arg(arg!(
            -p --prefix <PREFIX> "A file name prefix to filter."
        ))
        .arg(arg!(
            -f --filter <FILTER> "Json fields to filter, e.g gtp-handler,gx-client."
        ).required(false)
        )
        .get_matches();
    

    let dir_path: &PathBuf = matches.get_one::<PathBuf>("directory").unwrap();
    let prefix: &str = matches.get_one::<String>("prefix").unwrap();
    let filter = matches.get_one::<String>("filter");

    let mut q: Vec<PathBuf> = vec![];

    let mut entries = fs::read_dir(&dir_path.as_path()).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name().to_string_lossy().to_string();
        if file_name.contains(&prefix) {
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
        let mut prefix = "";
        process_json_value(&v, &prefix).await;
    }

    Ok(())
}

#[async_recursion]
async fn process_json_value(v: &Value, prefix: &str) -> io::Result<()> {
    
    match v {
        Value::Null  => {
            println!("Null value")
        },
        Value::Bool(value) => {
            println!("value {:?}", value)
        },
        Value::Number(value) => {
            println!("value {:?}", value)
        },
        Value::String(value) => {
            println!("value {:?}", value)
        },
        Value::Array(values) => { 
            for value in values.iter() {
                process_json_value(&value, &prefix);
            }
        },
        Value::Object(map) => { 
            for (key, value) in map.iter() {
                let new_prefix;
                if "" != prefix {
                    new_prefix = String::from(prefix) + "."  + key;
                } else {
                    new_prefix = String::from(key);

                }
                print!("entry {:?} ", new_prefix);
                process_json_value(&value, &new_prefix).await;
            }
        },
    }
    Ok(())

}