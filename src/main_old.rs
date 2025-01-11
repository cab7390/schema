use std::{
    collections::{HashMap, HashSet}, fs::File, hash::Hash, hint::black_box, io::{BufRead, BufReader}, time::Instant
};

pub mod path;

use path::run;
use rayon::{iter::{ParallelBridge, ParallelIterator}, str::ParallelString};
use schema::reader::ChunkedLineReader;

const MAX_STRING_SET_SIZE: usize = 10;
const MAX_OBJECT_SIZE: usize = 1000;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
enum Schema {
    #[default]
    Empty,
    Null,
    Boolean,
    String,
    StringSet(HashSet<String>),
    Number(NumberType),
    Array(Box<Schema>),
    EmptyArray,
    Object(HashMap<String, Schema>),
    Either(HashSet<Schema>),
    Optional(Box<Schema>),
    Generic,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum NumberType {
    I64,
    U64,
    F64,
}

impl Hash for Schema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Schema::Array(inner) | Schema::Optional(inner) => inner.hash(state),
            Schema::Object(map) => {
                for (key, value) in map {
                    key.hash(state);
                    value.hash(state);
                }
            }
            Schema::Either(set) => {
                for item in set {
                    item.hash(state);
                }
            }
            Schema::Number(inner) => inner.hash(state),
            Schema::StringSet(set) => {
                for item in set {
                    item.hash(state);
                }
            }
            _ => {} // Primitive types (Null, Boolean, etc.) don't need extra hashing
        }
    }
}

fn merge_schema(a: Schema, b: Schema) -> Schema {
    match (a, b) {
        (a, b) if a == b => a,

        (Schema::Empty, b) => b,
        (a, Schema::Empty) => a,

        (Schema::Null, b) => make_optional(b),
        (a, Schema::Null) => make_optional(a),

        (Schema::StringSet(mut a), Schema::StringSet(b)) => {
            // check max enum size
            if a.len() + b.len() > MAX_STRING_SET_SIZE {
                Schema::String
            } else {
                a.extend(b);
                Schema::StringSet(a)
            }
        }
        (Schema::String, Schema::StringSet(_)) => Schema::String,
        (Schema::StringSet(_), Schema::String) => Schema::String,
        (Schema::String, Schema::String) => Schema::String,

        (Schema::Optional(a), b) => make_optional(merge_schema(*a, b)),
        (a, Schema::Optional(b)) => make_optional(merge_schema(a, *b)),
        
        (Schema::Array(a), Schema::Array(b)) => Schema::Array(Box::new(merge_schema(*a, *b))),
        (Schema::EmptyArray, Schema::Array(b)) => Schema::Array(b),
        (Schema::Array(a), Schema::EmptyArray) => Schema::Array(a),
        (Schema::EmptyArray, Schema::EmptyArray) => Schema::EmptyArray,
        (Schema::Object(a), Schema::Object(b)) => {
            let merged = merge_object(a, b);
            if merged.len() > MAX_OBJECT_SIZE {
                Schema::Generic
            } else {
                Schema::Object(merged)
            }
            // Schema::Object(merge_object(a, b))
        }
        (_, Schema::Generic) => Schema::Generic,
        (Schema::Generic, _) => Schema::Generic,

        (Schema::Either(mut a), Schema::Either(b)) => {
            for schema in b {
                a.insert(schema);
            }
            Schema::Either(a)
        }
        (Schema::Either(mut a), b) => {
            a.insert(b);
            Schema::Either(a)
        }
        (a, Schema::Either(mut b)) => {
            b.insert(a);
            Schema::Either(b)
        }

        (a, b) => Schema::Either(HashSet::from([a, b])),
    }
}

fn merge_object(
    mut a: HashMap<String, Schema>,
    mut b: HashMap<String, Schema>,
) -> HashMap<String, Schema> {
    let mut merged = HashMap::new();

    // For each key in `a`, see if `b` has it too.
    for (key, a_val) in a.drain() {
        match b.remove(&key) {
            Some(b_val) => {
                // key exists in both
                let value = merge_schema(a_val, b_val);
                merged.insert(key, value);
            }
            None => {
                // only exists in `a`
                // FIX: only wrap in Optional if not already optional
                let value = make_optional(a_val);
                merged.insert(key, value);
            }
        }
    }

    // For any keys left in `b` that were not in `a`
    for (key, b_val) in b {
        // FIX: only wrap in Optional if not already optional
        let value = make_optional(b_val);
        merged.insert(key, value);
    }

    merged
}


fn unify_stringsets_in_either(schema: &mut Schema) {
    if let Schema::Either(set) = schema {
        // 1) gather all the string sets
        let (string_sets, non_string_sets): (Vec<_>, Vec<_>) =
            set.drain().partition(|s| matches!(s, Schema::StringSet(_)));

        // 2) unify them into one big set if any
        if !string_sets.is_empty() {
            let mut union: HashSet<String> = HashSet::new();
            for s in string_sets {
                if let Schema::StringSet(strings) = s {
                    union.extend(strings);
                }
            }
            // Possibly check max size, fallback to just `Schema::String`, etc.
            set.insert(Schema::StringSet(union));
        }

        // 3) put back all the non-string sets
        for s in non_string_sets {
            set.insert(s);
        }

        // You might also recursively unify further down
        // in case the `Either` contains other `Either`s, etc.
    } else {
        match schema {
            Schema::Array(inner) | Schema::Optional(inner) => unify_stringsets_in_either(inner),
            Schema::Object(map) => {
                for (_, value) in map {
                    unify_stringsets_in_either(value);
                }
            }
            _ => {}
        }
    }
}

fn make_optional(schema: Schema) -> Schema {
    match schema {
        Schema::Optional(_) => schema,
        _ => Schema::Optional(Box::new(schema)),
    }
}

// function for inferring schema from json
fn infer_schema(value: serde_json::Value) -> Schema {
    match value {
        serde_json::Value::Null => Schema::Null,
        serde_json::Value::Bool(_) => Schema::Boolean,
        serde_json::Value::Number(a) => {
            if a.is_i64() {
                Schema::Number(NumberType::I64)
            } else if a.is_u64() {
                Schema::Number(NumberType::U64)
            } else if a.is_f64() {
                Schema::Number(NumberType::F64)
            } else {
                unreachable!()
            }
        }
        // serde_json::Value::String(_) => Schema::String,
        serde_json::Value::String(string) => {
            let mut set = HashSet::new();
            set.insert(string);
            Schema::StringSet(set)
        }
        serde_json::Value::Array(array) => {
            if array.is_empty() {
                return Schema::EmptyArray;
            }

            let schemas = array.into_iter().map(infer_schema).collect::<Vec<_>>();
            let mut schema = schemas[0].clone();
            for other in schemas.into_iter().skip(1) {
                schema = merge_schema(schema, other);
            }
            Schema::Array(Box::new(schema))
        }
        serde_json::Value::Object(object) => {
            let schemas = object
                .into_iter()
                .map(|(key, value)| (key, infer_schema(value)))
                .collect::<HashMap<_, _>>();
            Schema::Object(schemas)
        }
    }
}

fn schema_gen() {
    let reader = BufReader::new(File::open("reddit.json").unwrap());
    let schema = reader
        .lines()
        .take(10_000_000)
        .enumerate()
        .map(|(i, line)| {
            if i % 10000 == 0 {
                println!("Processing line {}", i);
            }
            line
        })
        .par_bridge()
        .fold(
            // This closure creates the "per-thread accumulator"
            || Schema::Empty,
            // This closure merges the current line's schema into the thread-local schema
            |mut acc, line| {
                let line = line.unwrap();
                let value: serde_json::Value = serde_json::from_str(&line).unwrap();
                let inferred = infer_schema(value);
                acc = merge_schema(acc, inferred);
                acc
            },
        )
        .reduce(
            // This closure creates the "global accumulator" identity
            || Schema::Empty,
            // This closure merges any two partial results
            |a, b| {
                let mut merged = merge_schema(a, b);
                unify_stringsets_in_either(&mut merged);
                merged
            },
        );

    println!("{:#?}", schema);
}

fn main1() {
    // schema_gen();
    // run();

    let start = Instant::now();

    // let reader = BufReader::new(File::open("posts.json").unwrap());
    // let total = reader.lines().count();
    let reader = ChunkedLineReader::new("reddit.json", 5000).unwrap();
    reader.enumerate().take(100).par_bridge().for_each(|(i, chunk)| {
        let chunk = chunk.unwrap();
        println!("Processing chunk {}", i * 5000);
        chunk.into_iter().for_each(|line| {
            let mut bytes = line.into_bytes();
            let value = simd_json::to_borrowed_value(&mut bytes).unwrap();
            black_box(value);
            // let value: serde_json::Value = serde_json::from_str(&line).unwrap();
            // black_box(value);
        });
    });

    // let reader = JsonLines::new("posts.json", 1).unwrap();
    // let total = reader.count();
    
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed);
    // println!("Total: {}", total);

}

// merge them all by inserting into a binary tree and overloading the comparison operator

// use rayon to parallelize the process

// use serde_json to read the json file





