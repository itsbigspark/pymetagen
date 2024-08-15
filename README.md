# PyMetaGen

**PyMetaGen** is a powerful and fast data quality tool base on [Polars](https://pola.rs/#) designed for generating metadata and extracting useful information from various data file formats. It provides both a Python API and a command-line interface (CLI) to inspect, filter, and extract data from files such as CSV, JSON, Parquet, and Excel.

## Key Features

- **Metadata Generation**: Automatically generates metadata for your datasets, including statistics such as min, max, standard deviation, and more.
- **Data Extraction**: Easily extract specific rows from your datasets using head, tail, or random sampling.
- **Command Line Interface**: Perform operations like metadata generation, data inspection, and filtering using an intuitive CLI.
- **Multiple File Format Support**: Import and export data in various formats, including CSV, Parquet, Excel, and JSON.
- **SQL Query Support**: Filter data using SQL queries directly on the command line.

## Installation

To install the package, use the following command:

```bash
pip install pymetagen
```

## Local Installation

To install the package locally, use the following command:

```bash
python -m pip install -U git+ssh://git@github.com/smartdatafoundry/dotdda.git@dev/main
```

## Usage

### Python API

You can use the Python API to load a data file and generate metadata:

```python
from pymetagen import MetaGen

# Create an instance of the MetaGen class reading a data file

metagen = MetaGen.from_path("tests/data/testdata.csv", loading_mode="eager")

# Display the first few rows of the data

metagen.data.head()
```

```python
# Generate metadata and reset the index

metadata = metagen.compute_metadata().reset_index()

```

```python
# Save the metadata to a file

metagen.write_metadata("tests/data/testdata_metadata.csv")
```

### Command Line Interface

- **Metadata Generation** Generate metadata for a tabular data file:

```bash
$ metagen metadata -i tests/data/testdata.csv -o tests/data/testdata_metadata.csv

>>> Generating metadata for tests/data/testdata.csv...
```

- **Data Inspection** Inspect a data file (e.g., a partitioned Parquet file):

```bash
metagen inspect -i tests/data/input_ab_partition.parquet
```

- **Data Filtering** Filter a data set using an SQL query:

```bash
metagen filter -i tests/data/testdata.csv -q "SELECT * FROM data WHERE imdb_score > 9"
```

- **Data Extraction** Extract a specific number of rows from a data set:

```bash
$ metagen extracts -i tests/data/testdata.csv -o tests.csv -n 3

>>> Writing extract in: tests-head.csv
>>> Writing extract in: tests-tail.csv
>>> Writing extract in: tests-sample.csv
```

### Available Output Formats

- **CSV**
- **Parquet**
- **JSON**
- **Excel**
