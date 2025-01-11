use std::{
    fs::File,
    io::{BufRead, BufReader},
    thread::JoinHandle,
};

use flume::Receiver;

pub struct ChunkedLineReader {
    reader: std::io::BufReader<std::fs::File>,
    chunk_size: usize,
    line: String,
    chunk: Vec<String>,
}

impl ChunkedLineReader {
    pub fn new<P: AsRef<std::path::Path>>(path: P, chunk_size: usize) -> std::io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        Ok(Self {
            reader,
            chunk_size,
            line: String::new(),
            chunk: Vec::with_capacity(chunk_size),
        })
    }
}

impl Iterator for ChunkedLineReader {
    type Item = std::io::Result<Vec<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        for _ in 0..self.chunk_size {
            self.line.clear();
            match self.reader.read_line(&mut self.line) {
                Ok(0) => break,
                Ok(_) => self.chunk.push(std::mem::take(&mut self.line)),
                Err(e) => return Some(Err(e)),
            }
        }

        if self.chunk.is_empty() {
            None
        } else {
            Some(Ok(std::mem::take(&mut self.chunk)))
        }
    }
}