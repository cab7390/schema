use anyhow::Result;
use process::ParallelJsonProcessor;
use schema::Schema;

const CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16 MB

pub mod process;
pub mod schema;

fn main() -> Result<()> {
    let size = std::fs::metadata("posts.json")?.len();
    let start = std::time::Instant::now();
    let processer = ParallelJsonProcessor::new("posts.json", CHUNK_SIZE)?;

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
    println!("{:#?}", schema);
    println!(
        "Processed {} GiB in {:?}",
        size as f64 / 1024.0 / 1024.0 / 1024.0,
        elapsed
    );
    println!(
        "Throughput: {:.2} GiB/s",
        size as f64 / 1024.0 / 1024.0 / 1024.0 / elapsed.as_secs_f64()
    );
    println!("Processed {} records", processed);
    println!(
        "Throughput: {:.2} records/s",
        processed as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}
