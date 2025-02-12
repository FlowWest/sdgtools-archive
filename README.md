# sdgtools

Tools for processing DSM2 and HDF5 output. 

## Install

For now only development available for install until this repo is made public

```bash
# clone
git clone https://github.com/FlowWest/sdgtools

# move into
cd sdgtools

# install
pip install -e .

# or if using uv
uv pip install -e .
```

## Usage

sdgtools can be used both as a cli and a library. To use as a library simply import the usual way.

```python
from hecdss import HecDss
from sdgtools import get_all_data_from_dsm2_dss

filepath = "path/to/file.dss"

dss = HecDss(filepath)
data = get_all_data_from_dsm2_dss(dss)

# filter data to just certain nodes, we pass in a filter to part b of dss
data = get_all_data_from_dsm2_dss(dss, filter={"b": ["anh", "glf"]})
```

The corresponding operations in the CLI are:

```bash
sdgtools dss FPV1Ma_hydro_V7.dss output-file.csv
```
Filter to nodes of interest, this will filter to nodes "anh" and "clf" before processing.

```bash
  sdgtools dss --filter-location anh,clf FPV1Ma_hydro_V7.dss output-file.csv 
```

For cli help simply call `sdgtools --help`


## Database Inserts

Import data from CSV files directly into PostgreSQL database tables.

```
sdgtools db insert <csv_file> <scenario_name> <database_url>
```

### Parameters

- `csv_file`: Path to your CSV file to import
- `scenario_name`: Name of the target database table
- `database_url`: PostgreSQL connection URL in format: `postgresql://username:password@host:port/database`

### Example

```
sdgtools db insert fpv1ma_hydro_export.csv FPV1Ma postgresql://postgres:pass@sdg-db.example.com:5432/postgres
```

### Features

- Automatic table creation if it doesn't exist
- Column type inference from CSV data
- Chunked data insertion for memory efficiency
- Support for large CSV files



