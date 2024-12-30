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

Extract all data from a hydro dss file:

```bash
sdgtools hydro FPV1Ma_hydro_V7.dss output-file.csv
```
Filter to nodes of interest, this will filter to nodes "anh" and "clf" before processing.

```bash
sdgtools hydro --filter-location anh,clf FPV1Ma_hydro_V7.dss output-file.csv 
```



