use std::{
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::Parser;
use json_schema::RootJsonSchema;
use process::ParallelJsonProcessor;
use schema::{Config, Schema};

pub mod json_schema;
pub mod process;
pub mod schema;

fn process_file<P: AsRef<Path>>(path: P, config: &Config) -> Result<Schema> {
    let size = std::fs::metadata(path.as_ref())?.len();
    let start = std::time::Instant::now();
    let processer = ParallelJsonProcessor::new(path, config.chunk_size)?;

    let (processed, schema) = processer.process_with_thread_state(
        |json, (total, state): &mut (usize, Option<Schema>)| {
            *total += 1;
            match state {
                Some(schema) => {
                    let value_type = schema::infer_type(json, config);
                    schema.merge(value_type, config);
                }
                None => {
                    *state = Some(schema::infer_type(json, config));
                }
            }
        },
        |(x, a), (y, b)| match (a, b) {
            (Some(mut a), Some(b)) => {
                a.merge(b, config);
                (x + y, Some(a))
            }
            (Some(a), None) => (x + y, Some(a)),
            (None, Some(b)) => (x + y, Some(b)),
            (None, None) => unreachable!(),
        },
        || (0, None),
    );

    let elapsed = start.elapsed();

    if config.stats {
        eprintln!(
            "Processed {:.2} GiB in {:?}",
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
    }

    Ok(schema.expect("No schema found"))
}

fn main() -> Result<()> {
    let args = Args::parse();

    let config = Config {
        max_object_keys: args.max_object_keys,
        max_string_set_values: args.max_string_set_values,
        max_string_set_variant_length: args.max_string_set_variant_length,
        consider_string_set: args.consider_string_set,
        consider_array_items: args.consider_array_items,
        max_array_items: args.max_array_items,
        chunk_size: args.chunk_size,
        stats: args.stats,
    };

    let mut root_schema: Option<Schema> = match args.schema {
        Some(ref path) => {
            if path.exists() {
                eprintln!("Loading schema...");
                let schema = serde_json::from_reader(std::fs::File::open(path)?)?;
                Some(schema)
            } else {
                None
            }
        }
        None => None,
    };

    for path in args.file {
        let schema = process_file(path, &config)?;
        match root_schema {
            Some(ref mut root_schema) => {
                eprintln!("Merging schema...");
                root_schema.merge(schema, &config);
            },
            None => root_schema = Some(schema),
        }
    }

    let json_schema =
        RootJsonSchema::new(root_schema.clone().expect("No schema found. Did you provide any files?"));
    match args.output {
        Some(output) => {
            let mut output = BufWriter::new(std::fs::File::create(output)?);
            output.write_all(json_schema.to_string().as_bytes())?;
        }
        None => {
            println!("Generated schema:\n");
            println!("{}", json_schema);
        }
    }

    if let Some(schema) = args.schema {
        eprintln!("Writing schema to file...");
        let mut output = BufWriter::new(std::fs::File::create(schema)?);
        output.write_all(serde_json::to_string_pretty(&root_schema).unwrap().as_bytes())?;
    }

    Ok(())
}

#[derive(Debug, Parser, Clone)]
#[command(
    version,
    long_about = "A tool for inferring JSON schema from NDJSON files."
)]
struct Args {
    /// The file(s) to process.
    file: Vec<PathBuf>,

    /// The output file.
    /// If not provided, the schema will be printed to stdout.
    #[clap(long, short)]
    output: Option<PathBuf>,

    /// Path to load or merge an existing schema. If does not exist, a new schema will be created.
    /// If provided, the schema will be merged with the inferred schema
    #[clap(long)]
    schema: Option<PathBuf>,

    /// The maximum number of keys in an object before it is considered a large object.
    #[clap(long, default_value = "200")]
    max_object_keys: usize,

    /// The maximum number of values in a string set before it is considered just a string.
    #[clap(long = "max-enum-variants", default_value = "100")]
    max_string_set_values: usize,

    /// The maximum length of a string in a string set before it is considered just a string.
    #[clap(long = "max-enum-variant-len", default_value = "50")]
    max_string_set_variant_length: usize,

    /// Whether to consider enums (strings with a limited set of values).
    #[clap(long = "enums")]
    consider_string_set: bool,

    /// Whether to consider array items.
    #[clap(long = "array")]
    consider_array_items: bool,

    /// The maximum number of items in an array to process the schema for (sequential).
    #[clap(long = "max-array", default_value = "10")]
    max_array_items: usize,

    /// The size of the chunks to read from the file. (Default: 16 MiB)
    #[clap(long, default_value = "16777216")]
    chunk_size: usize,

    /// Display statistics after processing the file.
    #[clap(long)]
    stats: bool,
}
