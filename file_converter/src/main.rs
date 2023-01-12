use async_recursion::async_recursion;
use clap::{arg, command, value_parser, ArgAction};
use flate2::read::GzDecoder;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use salvo::macros::Extractible;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{self, PathBuf};
use tokio::fs::{self};
use tokio::io;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

use salvo::prelude::*;

/// model to parse json files and export to csv
/// dir_path - directory to parse files
/// prefix - prefix of json file names to select
/// output - path to output csv file
/// filter - to filter json fields
#[derive(Serialize, Deserialize, Extractible, Debug)]
#[extract(default_source(from = "body", format = "json"))]
struct PrepareStatisticsRequest<'a> {
    dir_path: &'a str,
    prefix: &'a str,
    output: &'a str,
    filter: Option<Vec<String>>,
}

/// model to scan folder and find json files
/// mask - mask to extract
///
#[derive(Serialize, Deserialize, Extractible, Debug)]
#[extract(default_source(from = "body", format = "json"))]
struct ScanDirectoriesRequest<'a> {
    path: &'a str,
}

/// parse json files and export to csv
/// retrieves PrepareStatisticsRequest model
#[handler]
async fn generate_statistics(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), anyhow::Error> {
    let statistics_request: PrepareStatisticsRequest = req.extract().await.unwrap();

    let path = PathBuf::from(statistics_request.dir_path);
    let prefix = String::from(statistics_request.prefix);
    let output = PathBuf::from(statistics_request.output);
    let filter = statistics_request.filter;

    tokio::spawn(scan_folders(path, prefix, output, filter));
    res.render("Completed");
    Ok(())
}

#[handler]
async fn scan_directories(req: &mut Request) -> String {
    let scan_request: ScanDirectoriesRequest = req.extract().await.unwrap();
    if let Ok(mut entries) = fs::read_dir(scan_request.path).await {
        while let Ok(entry) = entries.next_entry().await {
            if let Some(dir_entry) = entry {
                println!("{:?}: {}", dir_entry.file_name(), dir_entry.ino());
            }
        }
    }
    String::from("Completed")
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let router = Router::new()
        .push(Router::new().path("scan").post(generate_statistics))
        .push(Router::new().path("prepare").post(scan_directories));

    Server::new(TcpListener::bind("127.0.0.1:7878"))
        .serve(router)
        .await;

    //prepare(dir_path, prefix, output).await;

    Ok(())
}

async fn scan_folders(
    dir_path: PathBuf,
    prefix: String,
    output: PathBuf,
    filter: Option<Vec<String>>,
) -> io::Result<()> {
    let mut q: Vec<PathBuf> = vec![];

    let mut entries = fs::read_dir(&dir_path.as_path()).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name().to_string_lossy().to_string();
        if file_name.contains(&prefix) {
            q.push(entry.path());
        }
    }

    let json_paths: HashSet<&str> = HashSet::new();

    if let Some(filter_value) = filter {
        let filters = filter_value
            .iter()
            .map(|v| String::from(v.as_str()))
            .collect::<Vec<_>>();
        dispatch(q, filters, json_paths, &output).await;
    }

    Ok(())
}

async fn dispatch(
    list_of_files: Vec<PathBuf>,
    filters: Vec<String>,
    json_paths: HashSet<&str>,
    output: &PathBuf,
) -> io::Result<()> {
    let mut files = list_of_files.into_iter().tuples();
    let (tx1, mut rx) = mpsc::channel::<String>(10000000);

    tokio::spawn(async move {
        let mut is_header_extracted: bool = false;

        for (prev, next) in files.by_ref() {
            let file_with_headers = prev.clone();

            let paths = vec![prev, next].into_iter();

            if !is_header_extracted {
                try_to_extract_header_and_validate_json_paths(
                    file_with_headers,
                    tx1.clone(),
                    &filters,
                )
                .await;
                is_header_extracted = true;
            }

            let tasks = paths
                .map(|path| {
                    tokio::spawn(try_to_extract_and_process(
                        path,
                        tx1.clone(),
                        filters.clone(),
                    ))
                })
                .collect::<FuturesUnordered<_>>();

            futures::future::join_all(tasks).await;
        }
        for leftover in files.into_buffer() {
            if !is_header_extracted {
                try_to_extract_header_and_validate_json_paths(
                    leftover.clone(),
                    tx1.clone(),
                    &filters,
                )
                .await;
                is_header_extracted = true;
            }

            try_to_extract_and_process(leftover, tx1.clone(), filters.clone()).await;
        }
    });

    let mut file = File::create(output.as_path()).unwrap();
    while let Some(message) = rx.recv().await {
        writeln!(&mut file, "{}", message).unwrap();
    }

    Ok(())
}

// extract headers
async fn try_to_extract_header_and_validate_json_paths(
    entry: PathBuf,
    sender: Sender<String>,
    filters: &Vec<String>,
) -> io::Result<()> {
    if let Some(e) = entry.extension() {
        let file_extension = e.to_string_lossy();
        if file_extension.to_string() == "gz" {
            let mut d = GzDecoder::new(File::open(&entry).unwrap());
            let mut s = String::new();
            d.read_to_string(&mut s)?;
            process_text_lines_to_extract_header(s.as_str(), &sender, filters).await;
        } else {
            let file = File::open(&entry)?;
            let mut reader = BufReader::new(file);
            let mut s = String::new();
            reader.read_to_string(&mut s)?;
            process_text_lines_to_extract_header(s.as_str(), &sender, filters).await;
        }
    }
    Ok(())
}

async fn try_to_extract_and_process(
    entry: PathBuf,
    sender: Sender<String>,
    filter: Vec<String>,
) -> io::Result<()> {
    if let Some(e) = entry.extension() {
        let file_extension = e.to_string_lossy();
        if file_extension.to_string() == "gz" {
            let mut d = GzDecoder::new(std::fs::File::open(&entry).unwrap());
            let mut s = String::new();
            d.read_to_string(&mut s)?;
            process_text_lines(s.as_str(), &sender, &filter).await;
        } else {
            let file = File::open(&entry)?;
            let mut reader = BufReader::new(file);
            let mut s = String::new();
            reader.read_to_string(&mut s)?;
            process_text_lines(s.as_str(), &sender, &filter).await;
        }
    }
    Ok(())
}

async fn process_text_lines_to_extract_header(
    lines: &str,
    sender: &Sender<String>,
    filters: &Vec<String>,
) -> io::Result<()> {
    let mut headers: Vec<String> = vec![];
    if let Some(line) = lines.lines().next() {
        let root: Value = serde_json::from_str(line)?;
        for filter in filters {
            if let Some(found_value) = root.pointer(filter) {
                extract_json_headers(filter.as_str(), &found_value, &"", 0, &mut headers).await;
            }
        }

        let mut vector_of_headers = headers
            .into_iter()
            .map(|elem| {
                if let Some(_) = elem.find("/") {
                    (&elem[1..]).replace("/", "_")
                } else {
                    elem
                }
            })
            .collect::<Vec<String>>();
        vector_of_headers.insert(0, String::from("timestamp"));
        let header_line = vector_of_headers.join(",");
        sender.send(header_line).await;
    }
    Ok(())
}

async fn process_text_lines(
    lines: &str,
    sender: &Sender<String>,
    filters: &Vec<String>,
) -> io::Result<()> {
    for line in lines.lines() {
        let root: Value = serde_json::from_str(line)?;
        let mut rows: Vec<String> = vec![];
        if let Some(found_value) = root.pointer("/timestamp") {
            extract_json_rows(found_value, &mut rows).await;
        }

        for filter in filters {
            if let Some(found_value) = root.pointer(filter) {
                extract_json_rows(found_value, &mut rows).await;
            }
        }
        sender.send(rows.join(",").to_string()).await;
    }
    Ok(())
}

#[async_recursion]
async fn extract_json_headers(
    filter: &str,
    v: &Value,
    prefix: &str,
    index: usize,
    headers: &mut Vec<String>,
) -> io::Result<()> {
    match v {
        Value::Array(values) => {
            for (i, value) in values.iter().enumerate() {
                extract_json_headers(filter, &value, prefix, i + 1, headers).await;
            }
        }
        Value::Object(map) => {
            for (key, value) in map.iter() {
                let mut new_prefix: String;

                if prefix == "" {
                    new_prefix = String::from("/") + key;
                } else {
                    new_prefix = String::from(prefix) + "/" + key.as_str();
                }
                if index != 0 {
                    new_prefix = new_prefix + index.to_string().as_str();
                }
                extract_json_headers(filter, &value, new_prefix.as_str(), index, headers).await;
            }
        }
        _ => {
            let full_prefix = filter.to_owned() + prefix;
            println!("{:?}", full_prefix);
            headers.push(full_prefix.to_string());
        }
    }
    Ok(())
}

#[async_recursion]
async fn extract_json_rows(v: &Value, rows: &mut Vec<String>) -> io::Result<()> {
    match v {
        Value::Array(values) => {
            for (i, value) in values.iter().enumerate() {
                extract_json_rows(&value, rows).await;
            }
        }
        Value::Object(map) => {
            for (key, value) in map.iter() {
                extract_json_rows(&value, rows).await;
            }
        }
        Value::Null => {}
        Value::Bool(boolValue) => {
            rows.push(boolValue.to_string());
        }
        Value::Number(numberValue) => {
            rows.push(numberValue.to_string());
        }
        Value::String(stringValue) => {
            rows.push(stringValue.to_string());
        }
    }
    Ok(())
}
