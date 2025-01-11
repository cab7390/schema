use std::{any::Any, collections::{HashMap, HashSet}, fs::File, io::{BufRead, BufReader, Read}};

use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValueType {
    String,
    Number,
    Boolean,
    Null,
    Array,
    Object
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaType {
    pub types: HashSet<ValueType>,
    pub optional: bool,
    pub string_set: Option<HashSet<String>>,
}

fn initialize_schema(value: &serde_json::Value) -> SchemaType {
    SchemaType {
        types: HashSet::from([convert_type(value)]),
        optional: value.is_null(),
        string_set: None,
    }
}

pub fn combine_string_sets(a: &Option<HashSet<String>>, b: &Option<HashSet<String>>) -> Option<HashSet<String>> {
    match (a, b) {
        (None, None) => None,
        (None, Some(variants)) | (Some(variants), None) => {
            if variants.len() > SOME_MAX_VARIANTS {
                None
            } else {
                Some(variants.clone())
            }
        },
        (Some(variants_a), Some(variants_b)) => {
            let mut combined = variants_a.clone();
            combined.extend(variants_b.iter().cloned());
            if combined.len() > SOME_MAX_VARIANTS {
                None
            } else {
                Some(combined)
            }
        },
    }
}

// first
// pub fn merge_schemas(a: &SchemaType, b: &SchemaType) -> SchemaType {
//     let mut combined_types = a.types.clone();
//     combined_types.extend(b.types.iter().cloned());

//     let combined_optional = a.optional || b.optional;
//     let combined_string_set = combine_string_sets(&a.string_set, &b.string_set);

//     SchemaType {
//         types: combined_types,
//         optional: combined_optional,
//         string_set: combined_string_set,
//     }
// }

pub fn merge_schemas(a: &SchemaType, b: &SchemaType) -> SchemaType {
    let mut combined_types = a.types.clone();
    combined_types.extend(b.types.iter().cloned());

    // A field is optional if it is optional in either schema
    let combined_optional = a.optional || b.optional;

    let combined_string_variants = match (&a.string_set, &b.string_set) {
        (Some(variants_a), Some(variants_b)) => {
            let mut combined = variants_a.clone();
            combined.extend(variants_b.iter().cloned());

            if combined.len() > SOME_MAX_VARIANTS {
                None // Exceeded limit; switch to generic string
            } else {
                Some(combined)
            }
        }
        (Some(variants), None) | (None, Some(variants)) => {
            if variants.len() > SOME_MAX_VARIANTS {
                None // Exceeded limit; switch to generic string
            } else {
                Some(variants.clone())
            }
        }
        (None, None) => None,
    };

    SchemaType {
        types: combined_types,
        optional: combined_optional,
        string_set: combined_string_variants,
    }
}

// newer
// pub fn merge_schemas(a: &SchemaType, b: &SchemaType) -> SchemaType {
//     let mut combined_types = a.types.clone();
//     combined_types.extend(b.types.iter().cloned());
//     // A field is optional only if it is marked as optional in either schema
//     let combined_optional = a.optional || b.optional;
//     let combined_string_variants = match (&a.string_set, &b.string_set) {
//         (Some(variants_a), Some(variants_b)) => {
//             let mut combined = variants_a.clone();
//             combined.extend(variants_b.iter().cloned());
//             if combined.len() > SOME_MAX_VARIANTS {
//                 None // Exceeded limit; switch to generic string
//             } else {
//                 Some(combined)
//             }
//         }
//         (Some(variants), None) | (None, Some(variants)) => {
//             if variants.len() > SOME_MAX_VARIANTS {
//                 None // Exceeded limit; switch to generic string
//             } else {
//                 Some(variants.clone())
//             }
//         }
//         (None, None) => None,
//     };
//     SchemaType {
//         types: combined_types,
//         optional: combined_optional,
//         string_set: combined_string_variants,
//     }
// }

const SOME_MAX_VARIANTS: usize = 10;

fn infer_schema_entry(
    value: &serde_json::Value,
    schema: &mut HashMap<String, SchemaType>,
    parent_key: Option<&str>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                let full_key = parent_key.map_or_else(|| key.clone(), |p| format!("{}.{}", p, key));
                let entry = schema.entry(full_key.clone()).or_insert_with(|| initialize_schema(value));
                *entry = merge_schemas(entry, &initialize_schema(value));
                infer_schema_entry(value, schema, Some(&full_key));
            }
        }
        serde_json::Value::Array(array) => {
            // if array.len() > MAX_ARRAY_LENGTH {
                let full_key = parent_key.unwrap_or_default().to_string();
                let entry = schema.entry(full_key).or_insert_with(|| initialize_schema(value));
                entry.types.insert(ValueType::Array);
                return;
            // }

            for (index, element) in array.iter().enumerate() {
                let array_key = parent_key.map_or_else(|| index.to_string(), |p| format!("{}.{}", p, index));
                infer_schema_entry(element, schema, Some(&array_key));
            }
        }
        serde_json::Value::String(s) => {
            if let Some(key) = parent_key {
                let entry = schema.entry(key.to_string()).or_insert_with(|| initialize_schema(value));
                entry.types.insert(ValueType::String);

                if entry.string_set.is_none() {
                    entry.string_set = Some(HashSet::new());
                }

                if let Some(variants) = &mut entry.string_set {
                    variants.insert(s.clone());
                    if variants.len() > SOME_MAX_VARIANTS {
                        entry.string_set = None; // Exceeded limit; switch to generic string
                    }
                }
            } else {
                panic!("Top-level string value must have a key");
            }
        }
        _ => {
            if let Some(key) = parent_key {
                let entry = schema.entry(key.to_string()).or_insert_with(|| initialize_schema(value));
                *entry = merge_schemas(entry, &initialize_schema(value));
            } else {
                panic!("Top-level value must be an object or array");
            }
        }
    }
}

const MAX_ARRAY_LENGTH: usize = 20;

pub fn convert_array_inner(
    value: &[serde_json::Value],
    output: &mut HashMap<String, ValueType>,
    array_metadata: &mut HashMap<String, HashSet<ValueType>>, // Store aggregated types for arrays
    parent_key: Option<&'_ str>,
) {
    let key = parent_key.unwrap_or_default().to_string();

    // Ensure the parent key exists in the output
    output.insert(key.clone(), ValueType::Array);

    // Initialize or update the metadata for this array
    // let types = array_metadata.entry(key).or_insert_with(HashSet::new);

    todo!("AAAAA");
    if value.len() > MAX_ARRAY_LENGTH {
        // If the array is too large, treat it as a generic array
        array_metadata.entry(key.clone()).or_default().insert(ValueType::Array);
        return;
    }

    for element in value {
        let element_type = convert_type(element);
        // excessive clone of string to avoid lifetime issues
        array_metadata.entry(key.clone()).or_default().insert(element_type);

        // If the element is an object or array, recurse without adding to PathType
        match element {
            serde_json::Value::Array(inner_array) => {
                convert_array_inner(inner_array, output, array_metadata, parent_key);
            }
            serde_json::Value::Object(_) => {
                convert_object_inner(element, output, parent_key);
            }
            _ => {} // Primitive types are already handled
        }
    }
}

pub fn convert_object_inner(
    value: &serde_json::Value,
    output: &mut HashMap<String, ValueType>,
    parent_key: Option<&'_ str>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                let key = parent_key
                    .map(|parent_key| format!("{}.{}", parent_key, key))
                    .unwrap_or(key.clone());
                convert_object_inner(value, output, Some(&key));
            }
        },
        serde_json::Value::Array(array) => {
            // let mut array_metadata = HashMap::new();
            // println!("Array Len: {:?}", array.len());
            // convert_array_inner(array, output, &mut array_metadata, parent_key);
        },
        other => {
            let value_type = convert_type(other);
            if let Some(parent_key) = parent_key {
                output.insert(parent_key.to_string(), value_type);
            } else {
                output.insert("".to_string(), value_type);
            }
        }
    }
}

pub fn convert_object(value: &serde_json::Value) -> HashMap<String, ValueType> {
    let mut output = HashMap::new();
    convert_object_inner(value, &mut output, None);
    output
}

pub fn convert_type(value: &serde_json::Value) -> ValueType {
    match value {
        serde_json::Value::String(_) => ValueType::String,
        serde_json::Value::Number(_) => ValueType::Number,
        serde_json::Value::Bool(_) => ValueType::Boolean,
        serde_json::Value::Null => ValueType::Null,
        serde_json::Value::Array(_) => ValueType::Array,
        serde_json::Value::Object(_) => ValueType::Object,
        _ => panic!("Expected string, number, boolean, null, or array"),
    }
}

pub fn process_file_parallel(file_path: &str) -> HashMap<String, SchemaType> {
    // Open the file and set up the parallel processing
    let file = std::fs::File::open(file_path).unwrap();
    let reader = std::io::BufReader::new(file);

    // Use `par_bridge` for parallel processing of lines
    let partial_schemas: Vec<HashMap<String, SchemaType>> = reader
        .lines()
        .enumerate()
        .par_bridge()
        .map(|(i, line)| {
            if i % 10000 == 0 {
                println!("Processing line {}", i);
            }
            
            let line = line.unwrap();
            let value: serde_json::Value = serde_json::from_str(&line).unwrap();
            let mut schema = HashMap::new();
            infer_schema_entry(&value, &mut schema, None);
            schema
        })
        .collect();

    // Merge all partial schemas
    partial_schemas.into_iter().reduce(merge_schema_maps).unwrap_or_default()
}
// Fixed chunk size
const CHUNK_SIZE: usize = 10_000;

pub struct ChunkedLineReader {
    reader: BufReader<File>,
    chunk_size: usize,
    lines_read: usize,
}

impl ChunkedLineReader {
    pub fn new<P: AsRef<std::path::Path>>(file_path: P, chunk_size: usize) -> std::io::Result<Self> {
        let file = File::open(file_path)?;
        Ok(Self {
            reader: BufReader::new(file),
            chunk_size,
            lines_read: 0,
        })
    }
}

impl Iterator for ChunkedLineReader {
    type Item = std::io::Result<Vec<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = Vec::with_capacity(self.chunk_size);
        for _ in 0..self.chunk_size {
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => break, // End of file reached
                Ok(_) => chunk.push(line.trim_end().to_string()),
                Err(e) => return Some(Err(e)),
            }
        }

        self.lines_read += chunk.len();
        if self.lines_read % 10000 == 0 {
            println!("Processed {} lines", self.lines_read);
        }

        if chunk.is_empty() {
            None // No more data to read
        } else {
            Some(Ok(chunk))
        }
    }
}


pub fn process_file_incremental(file_path: &str) -> HashMap<String, SchemaType> {
    let chunks = ChunkedLineReader::new(file_path, CHUNK_SIZE).unwrap();

    // Process chunks in parallel and merge results incrementally
    chunks
        .take(100)
        .par_bridge()
        .map(|chunk| {
            let chunk = chunk.unwrap();
            process_chunk(&chunk)
        })
        .reduce(HashMap::new, merge_schema_maps)
}

// Process a single chunk
fn process_chunk(chunk: &[String]) -> HashMap<String, SchemaType> {
    let mut local_schema = HashMap::new();
    for line in chunk {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        infer_schema_entry(&value, &mut local_schema, None);
    }
    local_schema
}

// Merge schemas from multiple chunks
fn merge_schema_maps(
    mut map1: HashMap<String, SchemaType>,
    map2: HashMap<String, SchemaType>,
) -> HashMap<String, SchemaType> {
    for (key, schema2) in map2 {
        let schema1 = map1.remove(&key).unwrap_or_else(|| SchemaType {
            types: HashSet::new(),
            optional: true,
            string_set: None,
        });
        map1.insert(key, merge_schemas(&schema1, &schema2));
    }
    map1
}
pub fn run() {
    let sample = r#"{"id":1,"created_at":"2007-07-16T05:19:58Z","score":609,"md5":"f3824ad985f121187065c4eaeae22875","directory":"f3/82","image":"70aa920c2045b4b72da6d778b8be1ecf0e734f8a.jpg","rating":"Safe","change":1710476249,"owner":"danbooru","creator_id":6498,"preview":{"url":"https://img3.gelbooru.com/thumbnails/f3/82/thumbnail_f3824ad985f121187065c4eaeae22875.jpg","width":166,"height":250},"original":{"url":"https://img3.gelbooru.com/images/f3/82/f3824ad985f121187065c4eaeae22875.jpg","width":400,"height":600},"tags":["1girl","apron","asahina_mikuru","asahina_mikuru_(cosplay)","asian","breasts","brown_eyes","brown_hair","closed_mouth","corset","cosplay","cosplay_photo","crossed_legs","dress","dyed_hair","female_focus","from_above","get","hairband","hand_on_own_face","hand_up","head_tilt","indoors","japanese_(nationality)","lips","long_hair","looking_at_viewer","maid","maid_headdress","medium_breasts","mikuru_beam","mizuhara_arisa","name_tag","pantyhose","peace_symbol","photo_(medium)","pink_dress","pink_theme","puffy_short_sleeves","puffy_sleeves","real_life","short_sleeves","sitting","smile","solo","suzumiya_haruhi_no_yuuutsu","translated","v","v_over_eye","waitress","wrist_cuffs"],"has_notes":false,"has_comments":true,"status":"active","post_locked":false,"has_children":false}"#;

    let value: serde_json::Value = serde_json::from_str(sample).unwrap();
    let output = convert_object(&value);
    println!("{:#?}", output);

    let schema = process_file_incremental("reddit.json");
    println!("{:#?}", schema);

    // Read from file
    // let reader = BufReader::new(File::open("tags.json").unwrap());
    // reader.lines().enumerate().par_bridge().for_each(|(i, line)| {
    //     if i % 10000 == 0 {
    //         println!("Processing line {}", i);
    //     }
    //     let line = line.unwrap();
    //     let value: serde_json::Value = serde_json::from_str(&line).unwrap();
    //     let output = convert_object(&value);
    // });
}
