# PyMetaGen Usage Guide

## Introduction

**PyMetaGen** is a versatile tool that can be used both through its Python API and from the command line to manage and extract metadata from various data files. Below are detailed instructions on how to use the tool in different scenarios.

## Command Line Interface

### Metadata Command

The `metadata` command generates metadata for a given input file.

#### Example Usage

```bash
metagen metadata -i tests/data/testdata.csv -o tests/data/testdata_metadata.csv
```

This command generates a metadata file from testdata.csv and saves it to testdata_metadata.csv.

Options:

- `-i`, `--input` PATH - Required: Path to the input file. Supports .csv, .parquet, .xlsx, .json.
- `-o`, `--output` FILE - Output file path. Supports .csv, .parquet, .xlsx, .json.
- `-d`, `--descriptions` FILE - Path to a JSON file containing descriptions for each column.
- `-m`, `--loading-mode` [lazy|eager] - Whether to load data in lazy or eager mode. Defaults to lazy.
- `-xfmt`, `--extra-formats` TEXT - Additional output formats separated by commas (e.g., .csv,.parquet).
- `-show-desc`, `--show-descriptions` - Print column descriptions to the console.
- `-P`, `--preview` - Preview the metadata file (OS-specific).
- `-warn-desc`, `--warning-description` - Force descriptions for all columns.
- `-h`, `--help` - Show the help message and exit.

### Inspect Command

The inspect command allows you to view and inspect data files, particularly useful for partitioned Parquet files.

#### Example Usage

```bash
metagen inspect -i tests/data/input_ab_partition.parquet
```

Options:

- `-i`, `--input` PATH - Required: Path to the input file.
- `-o`, `--output` FILE - Output file path.
- `-m`, `--loading-mode` [lazy|eager] - Whether to load data in lazy or eager mode. Defaults to lazy.
- `-n`, `--number-rows` INTEGER - Maximum number of rows to show. Defaults to 10.
- `-P`, `--preview` - Preview the file (OS-specific).
- `--fmt-str-lengths` INTEGER - Maximum number of characters per string column. Defaults to 50.
- `-im,` `--inspection-mode` [head|tail|sample] - Inspection mode: head, tail, or random sample. Defaults to head.
- `--random-seed` INTEGER - Seed for random sampling. Defaults to None.
- `-wr`, `--with-replacement` - Allow sampling with replacement. Defaults to False.
- `-h`, `--help` - Show the help message and exit.

### Filter Command

The filter command filters data sets based on SQL queries.

#### Example Usage

```bash
metagen filter -i tests/data/testdata.csv -q "SELECT * FROM data WHERE imdb_score > 9"
```

Options

- `-i`, `--input` PATH - Required: Path to the input file.
- `-t`, `--table-name` TEXT - Table name for SQL queries. Defaults to the input file name.
- `-o`, `--output` FILE - Output file path.
- `-q`, `--query` TEXT - Required: SQL query string/file to filter the data.
- `-m`, `--loading-mode` [lazy|eager] - Whether to load data in lazy or eager mode. Defaults to lazy.
- `-P`, `--preview` - Preview the filtered data file (OS-specific).
- `-h`, `--help` - Show the help message and exit.

Extracts Command
The extracts command allows you to extract specific rows from a data set.

Example Usage

```bash
metagen extracts -i tests/data/testdata.csv -o tests.csv -n 3
```

Options

- `-i`, `--input` PATH - Required: Path to the input file.
- `-o`, `--output` FILE - Required: Output file path.
- `-m`, `--loading-mode` [lazy|eager] - Whether to load data in lazy or eager mode. Defaults to lazy.
- `-n`, `--number-rows` INTEGER - Maximum number of rows to extract. Defaults to 10.
- `--random-seed` INTEGER - Seed for random sampling. Defaults to None.
- `-wr`, `--with-replacement` - Allow sampling with replacement. Defaults to False.
- `-xfmt`, `--extra-formats` TEXT - Additional output formats separated by commas (e.g., .csv,.parquet).
- `-ignore-im`, `--ignore-inspection-modes` TEXT - Comma-separated list of inspection modes to ignore (head, tail, sample).
- `-h`, `--help` - Show the help message and exit.
