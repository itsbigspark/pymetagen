# PyMetaGen

Metadata Generator

The objective of this library is to generate a useful metadata from a given
data file type:

- **PARQUET**
- **JSON**
- **CSV**
- **EXCEL**

## Installation

The following command will install the package:

```bash
pip install pymetagen
```

## Local Installation

The following command will install the package locally:

```bash
 python -m pip install -U git+ssh://git@github.com/smartdatafoundry/dotdda.git@dev/main
```

## Usage

The following command will load a data file:

```python
from pymetagen import MetaGen

# Create an instance of the MetaGen class reading a data file
metagen = MetaGen.from_path("tests/data/testdata.csv", loading_mode="eager")

metagen.data.head()
```

<div>
<small>shape: (5, 5)</small>
    <table border="1" class="dataframe">
        <thead>
            <tr><th>title</th><th>release_year</th><th>budget</th><th>gross</th><th>imdb_score</th></tr>
            <tr><td>str</td><td>i64</td><td>i64</td><td>i64</td><td>f64</td></tr>
        </thead>
        <tbody>
            <tr><td>&quot;The Godfather&quot;</td><td>1972</td><td>6000000</td><td>134821952</td><td>9.2</td></tr>
            <tr><td>&quot;The Dark Knight&quot;</td><td>2008</td><td>185000000</td><td>533316061</td><td>9.0</td></tr>
            <tr><td>&quot;Schindler&#x27;s List&quot;</td><td>1993</td><td>22000000</td><td>96067179</td><td>8.9</td></tr>
            <tr><td>&quot;Pulp Fiction&quot;</td><td>1994</td><td>8000000</td><td>107930000</td><td>8.9</td></tr>
            <tr><td>&quot;The Shawshank Redemption&quot;</td><td>1994</td><td>25000000</td><td>28341469</td><td>9.3</td></tr>
        </tbody>
    </table>
</div>

The metadata can be generated using the following command:

```python
metagen.compute_metadata().reset_index()
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>Name</th>
      <th>Long Name</th>
      <th>Type</th>
      <th>Description</th>
      <th>Min</th>
      <th>Max</th>
      <th>Std</th>
      <th>Min Length</th>
      <th>Max Length</th>
      <th># nulls</th>
      <th># empty/zero</th>
      <th># positive</th>
      <th># negative</th>
      <th># unique</th>
      <th>Values</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>title</td>
      <td></td>
      <td>string</td>
      <td></td>
      <td>Pulp Fiction</td>
      <td>The Shawshank Redemption</td>
      <td>None</td>
      <td>12.0</td>
      <td>24.0</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>5</td>
      <td>[Pulp Fiction, Schindler's List, The Dark Knig...</td>
    </tr>
    <tr>
      <td>release_year</td>
      <td></td>
      <td>integer</td>
      <td></td>
      <td>1972.0</td>
      <td>2008.0</td>
      <td>12.891858</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>4</td>
      <td>[1972, 1993, 1994, 2008]</td>
    </tr>
    <tr>
      <td>budget</td>
      <td></td>
      <td>integer</td>
      <td></td>
      <td>6000000.0</td>
      <td>185000000.0</td>
      <td>76372115.330139</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>5</td>
      <td>[6000000, 8000000, 22000000, 25000000, 185000000]</td>
    </tr>
    <tr>
      <td>gross</td>
      <td></td>
      <td>integer</td>
      <td></td>
      <td>28341469.0</td>
      <td>533316061.0</td>
      <td>201315897.848754</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>5</td>
      <td>[28341469, 96067179, 107930000, 134821952, 533...</td>
    </tr>
    <tr>
      <td>imdb_score</td>
      <td></td>
      <td>float</td>
      <td></td>
      <td>8.9</td>
      <td>9.3</td>
      <td>0.181659</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>4</td>
      <td>[8.9, 9.0, 9.2, 9.3]</td>
    </tr>
  </tbody>
</table>
</div>

Alternatively, the metadata can be saved to a file using the following command:

```python
metagen.write_metadata("tests/data/testdata_metadata.csv")
```

The available output formats are:

- **CSV**
- **PARQUET**
- **JSON**
- **EXCEL**

## CLI Usage

The command line interface have the following commands:

- `metadata`:  A tool to generate metadata for tabular data.
- `inspect`:   A tool to inspect a data set.
- `filter`:    A tool to filter a data set.
- `extracts`:  A tool to extract `n` number of rows from a data set.

### Metadata

A tool to generate metadata for tabular data.

The following command:

```bash
metagen metadata -i tests/data/testdata.csv -o tests/data/testdata_metadata.csv

>>> Generating metadata for tests/data/testdata_metadata.csv...
```

will generate a metadata file. The output file is not required, when no output
file is provided, the metadata will be printed to the console. Also, there is
an option to open a preview of the metadata file using the `-P` flag, this option
only works for OS systems that have a default application to open the file.

The metadata command has the following options:

```bash
Options:

  --version                       Show the version and exit.
  -i, --input PATH                Input file path. Can be of type: .csv,
                                  .parquet, .xlsx, .json  [required]
  -o, --output FILE               Output file path. Can be of type: .csv,
                                  .parquet, .xlsx, .json
  -d, --descriptions FILE         (optional) Path to a JSON file containing
                                  descriptions for each column.
  -m, --loading-mode [lazy|eager]
                                  (optional) Whether to use lazy or eager
                                  mode. Defaults to lazy.
  -xfmt, --extra-formats TEXT     Output file formats. Can be of type: .csv,
                                  .parquet, .xlsx, .json or combinations of
                                  them, separated by commas, e.g
                                  '.csv,.parquet,.json'.
  -show-desc, --show-descriptions
                                  (optional flag) Show columns descriptions
                                  printed in the console. Can be of type: True
                                  or False. Defaults to False.
  -P, --preview                   (optional flag) Opens a Quick Look Preview
                                  mode of the file. NOTE: Only works for OS
                                  operating systems). Defaults to False.
  -warn-desc, --warning-description
                                  (optional flag) in force descriptions for
                                  all columns.
  -h, --help                      Show this message and exit.
```

### Inspect

A tool to inspect/view a data set. The main purpose of this tool is to inspect partitioned
parquet files.

The following command:

```bash
metagen inspect -i tests/data/input_ab_partition.parquet
```

will generate the following output:

<img width="599" alt="inspect_data_ouput" src="https://github.com/user-attachments/assets/92e7310c-a6c0-4d72-b290-d5738fa43dac">

The inspect command has the following options:

```bash
Options:
  -i, --input PATH                Input file path. Can be of type: .csv,
                                  .parquet, .xlsx, .json  [required]
  -o, --output FILE               Output file path. Can be of type: .csv,
                                  .parquet, .xlsx, .json
  -m, --loading-mode [lazy|eager]
                                  (optional) Whether to use lazy or eager
                                  mode. Defaults to lazy.
  -n, --number-rows INTEGER       (optional) Maximum number of rows to show.
                                  Defaults to 10.
  -P, --preview                   (optional flag) Opens a Quick Look Preview
                                  mode of the file. NOTE: Only works for OS
                                  operating systems). Defaults to False.
  --fmt-str-lengths INTEGER       (optional) Maximum number of characters for
                                  string in a column. Defaults to 50.
  -im, --inspection-mode [head|tail|sample]
                                  (optional) Whether to use head, tail or a
                                  random sample inspection mode. Defaults to
                                  head.
  --random-seed INTEGER           (optional) Seed for the random number
                                  generator when the sample inspect mode
                                  option is activated. Defaults to None.
  -wr, --with-replacement         (optional flag) Allow values to be sampled
                                  more than once when the sample inspect mode
                                  option is activated. Defaults to False.
  -h, --help                      Show this message and exit.
```

### Filter

A tool to filter a data set. This command is useful to filter a data set based on
a SQL query, where the table name is the name of the file, by default.
The following command:

```bash
metagen filter -i tests/data/testdata.csv -q "SELECT * FROM data WHERE imdb_score > 9"
```

will generate the following output:

<img width="797" alt="filtered_data_output" src="https://github.com/user-attachments/assets/094c24b9-7b2e-4a9d-8c70-ab1abf3285c5">

It is important to note that the filter command needs a query to filter the data. It can
be a SQL query string or a file containing the query. The output file is not required,
when no output file is provided, the filtered data will be printed to the console. Also,
there is an option to open a preview of the filtered data file using the `-P` flag, this
option only works for OS systems that have a default application to open the file.

The filter command has the following options:

```bash
Options:
  -i, --input PATH                Input file path. Can be of type: .csv,
                                  .parquet, .xlsx, .json  [required]
  -t, --table-name TEXT           Name of the table to filter. Defaults to the
                                  input file name.
  -o, --output FILE               Output file path. Can be of type: .csv,
                                  .parquet, .xlsx, .json
  -q, --query TEXT                SQL query string/file to apply to the data.
                                  [required]
  -m, --loading-mode [lazy|eager]
                                  (optional) Whether to use lazy or eager
                                  mode. Defaults to lazy.
  -e, --eager BOOLEAN             (optional) Whether to use lazy or eager
                                  mode. Defaults to lazy.
  -P, --preview                   (optional flag) Opens a Quick Look Preview
                                  mode of the file. NOTE: Only works for OS
                                  operating systems). Defaults to False.
  -h, --help                      Show this message and exit.
```

### Extracts

A tool to extract n number of rows from a data set. It can extract head, tail,
and random sample at the same time.

The following command:

```bash
metagen extracts -i tests/data/testdata.csv -o tests.csv -n 3

>>> Writing extract in: tests-head.csv
>>> Writing extract in: tests-tail.csv
>>> Writing extract in: tests-sample.csv
```

will generate three files with the following content:

- The first 3 rows of the data set
- The last 3 rows of the data set
- A random sample of 3 rows from the data set

The extracts command has the following options:

```bash
Options:
  -i, --input PATH                Input file path. Can be of type: .csv,
                                  .parquet, .xlsx, .json  [required]
  -o, --output FILE               Output file path. Can be of type: .csv,
                                  .parquet, .xlsx, .json  [required]
  -m, --loading-mode [lazy|eager]
                                  (optional) Whether to use lazy or eager
                                  mode. Defaults to lazy.
  -n, --number-rows INTEGER       (optional) Maximum number of rows to show.
                                  Defaults to 10.
  --random-seed INTEGER           (optional) Seed for the random number
                                  generator when the sample inspect mode
                                  option is activated. Defaults to None.
  -wr, --with-replacement         (optional flag) Allow values to be sampled
                                  more than once when the sample inspect mode
                                  option is activated. Defaults to False.
  -xfmt, --extra-formats TEXT     Output file formats. Can be of type: .csv,
                                  .parquet, .xlsx, .json or combinations of
                                  them, separated by commas, e.g
                                  '.csv,.parquet,.json'.
  -ignore-im, --ignore-inspection-modes TEXT
                                  Comma-separated list of inspection modes to
                                  ignore. Can be of type: head, tail, sample.
  -h, --help                      Show this message and exit.
```
