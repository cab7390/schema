# Schema

A high-performance command-line tool for inferring JSON schemas from newline-delimited JSON (NDJSON) files. Designed to handle large datasets efficiently using parallel processing, memory mapping, and custom schema merging logic. The tool outputs a JSON Schema compatible with [Draft 2020-12](https://json-schema.org/draft/2020-12/schema).

## Features

- **High Performance**: Processes large files with parallelized JSON parsing using SIMD optimizations.
- **Configurable**: Supports customizable schema settings like maximum object keys, string set detection, and array item consideration.
- **Extensive Schema Support**: Generates comprehensive JSON schemas with merged, nested, and complex data types.
- **Statistics**: Provides detailed processing metrics.

## Installation

Clone the repository and build the binary:

```bash
git clone https://github.com/cab7390/schema.git
cd schema
cargo build --release
```

The compiled binary will be available in `target/release/schema`.

## Usage

### Command-Line Arguments

Run the tool with the following options:

```bash
schema [OPTIONS] --file <FILE>...
```

#### Options:
- `--file <FILE>` (required): One or more NDJSON files to process.
- `--output <OUTPUT>`: File to save the generated schema. Defaults to printing to `stdout`.
- `--schema <SCHEMA>`: Load or merge an existing schema. If the file doesn't exist, a new schema will be created.
- `--max-object-keys <N>`: Max keys in an object before it's treated as large (default: 200).
- `--max-enum-variants <N>`: Max unique string values in a set before it's treated as a string (default: 100).
- `--max-enum-variant-len <N>`: Max string length in a set before treating it as a string (default: 50).
- `--enums`: Enable detection of string sets (enum-like behavior).
- `--array`: Enable schema inference for array items.
- `--max-array <N>`: Max items in an array to process for schema inference (default: 10).
- `--chunk-size <SIZE>`: Chunk size (in bytes) for file processing (default: 16 MiB).
- `--stats`: Display processing statistics.

### Examples

#### Infer a schema and print to stdout
```bash
schema --file data.ndjson
```

#### Save the schema to a file
```bash
schema --file data.ndjson --output schema.json
```

#### Merge an existing schema with new data
```bash
schema --file data1.ndjson --file data2.ndjson --schema existing_schema.json --output updated_schema.json
```

#### Enable string set detection
```bash
schema --file data.ndjson --enums
```

#### Process large files with a custom chunk size
```bash
schema --file large_data.ndjson --chunk-size 33554432 --stats
```

## Output Format

The generated JSON Schema adheres to the [Draft 2020-12 standard](https://json-schema.org/draft/2020-12/schema). It includes details about object properties, array items, string sets, and other inferred data types.

Example Output:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "id": {
      "type": "string"
    },
    "values": {
      "type": "array",
      "items": {
        "type": "number"
      }
    }
  },
  "required": ["id"]
}
```
