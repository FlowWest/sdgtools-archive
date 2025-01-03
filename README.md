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
```

Extract all data from a hydro dss file:

```bash
sdgtools dss FPV1Ma_hydro_V7.dss output-file.csv
```
Filter to nodes of interest, this will filter to nodes "anh" and "clf" before processing.

```bash
  sdgtools dss --filter-location anh,clf FPV1Ma_hydro_V7.dss output-file.csv 
```



