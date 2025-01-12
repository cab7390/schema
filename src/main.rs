use std::io::{BufWriter, Write};

use anyhow::Result;
use json_schema::{JsonSchema, RootJsonSchema};
use process::ParallelJsonProcessor;
use schema::Schema;

const CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16 MB

pub mod json_schema;
pub mod process;
pub mod schema;

fn process_file(filename: &str) -> Result<Schema> {
    let size = std::fs::metadata(filename)?.len();
    let start = std::time::Instant::now();
    let processer = ParallelJsonProcessor::new(filename, CHUNK_SIZE)?;

    let (processed, schema) = processer.process_with_thread_state(
        |json, (total, state): &mut (usize, Option<Schema>)| {
            *total += 1;
            match state {
                Some(schema) => {
                    let value_type = schema::infer_type(json);
                    schema.merge(value_type);
                }
                None => {
                    *state = Some(schema::infer_type(json));
                }
            }
        },
        |(x, a), (y, b)| match (a, b) {
            (Some(mut a), Some(b)) => {
                a.merge(b);
                (x + y, Some(a))
            }
            (Some(a), None) => (x + y, Some(a)),
            (None, Some(b)) => (x + y, Some(b)),
            (None, None) => unreachable!(),
        },
        || (0, None),
    );

    let elapsed = start.elapsed();
    // println!("Total sum of 'count' fields in NDJSON: {}", sum);
    // eprintln!("{:#?}", schema);
    eprintln!(
        "Processed {} GiB in {:?}",
        size as f64 / 1024.0 / 1024.0 / 1024.0,
        elapsed
    );
    eprintln!(
        "Throughput: {:.2} GiB/s",
        size as f64 / 1024.0 / 1024.0 / 1024.0 / elapsed.as_secs_f64()
    );
    eprintln!("Processed {} records", processed);
    eprintln!(
        "Throughput: {:.2} records/s",
        processed as f64 / elapsed.as_secs_f64()
    );

    Ok(schema.expect("No schema found"))
}

fn main() -> Result<()> {
    // const FILES: [&str; 4] = [
    //     "data/Observation.008.ndjson",
    //     "data/Procedure.001.ndjson",
    //     "data/DocumentReference.002.ndjson",
    //     "data/DiagnosticReport.000.ndjson",
    // ];

    // for file in FILES.iter() {
    //     let schema = process_file(file)?;
    //     let json_schema = RootJsonSchema::new(schema);
    //     let mut output = BufWriter::new(std::fs::File::create(format!("{}.schema.json", file))?);
    //     output.write_all(json_schema.to_string().as_bytes())?;
    // }
    
    let schema = process_file("data/posts.json")?;
    // let json_schema = RootJsonSchema::new(schema);
    // let mut output = BufWriter::new(std::fs::File::create("schema2.json")?);
    // output.write_all(json_schema.to_string().as_bytes())?;

    Ok(())
}
