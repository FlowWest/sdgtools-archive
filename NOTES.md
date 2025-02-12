# Notes 

Here are some notes for the data processing of model output files

## DSM2 DSS Data

For each scenario we have a few different dss files. For example scenario "FPV1Ma" the files
for this model are:

| Filename                  | Type   | Description |
|---------------------------|--------| ------------|
| FPV1Ma.hof                 | text   | ? |
| FPV1Ma_CCF.dss             | binary | ? |
| FPV1Ma_SDG.dss             | binary | gate data | 
| FPV1Ma_hydro.dss           | binary | water level/stage data | 
| hydro_echo_FPV1Ma.inp      | text   | model metadata |


### SDG Data
The SDG data that corresponds to datasets of the form __FPV1Ma_SDG.dss__ are used to extract 
following data.

__DEVICE-FLOW__ - represents flows at the gates
- GLC_FLOW_FISH
- MID_FLOW_FISH
- MID_FLOW_GATE
- OLD_FLOW_FISH
- OLD_FLOW_GATE

__STAGE__ - represents gate elevations
- MID_GATE_UP
- MID_GATE_DOWN
- GLC_GATE_UP
- GLC_GATE_DOWN
- OLD_GATE_UP
- OLD_GATE_DOWN

__ELEV__ - represents gate operations
- MID_GATEOP
- GLC_GATEOP
- OLD_GATEOP

### HYDRO Data
The HYDRO data that corresponds to datasets of the form __FPV1Ma_hydro.dss__ are used to extract 
following data.

__STAGE__ - represents water level compliance
- MHO
- DGL
- OLD

Therefore using `sdgtools` we can do the following to get all this data at once:

```bash
sdgtools dss --location-filter GLC_FLOW_FISH,MID_FLOW_FISH,OLD_FLOW_FISH FPV1Ma_SDG_V7.dss fpv1ma_sdg_flow_export.csv
sdgtools dss --location-filter GLC_GATE_UP,MID_GATE_UP,OLD_GATE_UP FPV1Ma_SDG_V7.dss fpv1ma_sdg_gate_elevs_export.csv
sdgtools dss --location-filter MID_GATEOP,GLC_GATEOP,OLD_GATEOP FPV1Ma_SDG_V7.dss fpv1ma_sdg_gate_ops_export.csv
```

or if you want to do them all in one go:

```bash
sdgtools dss --location-filter GLC_FLOW_FISH,MID_FLOW_FISH,OLD_FLOW_FISH,GLC_GATE_UP,MID_GATE_UP,OLD_GATE_UP,MID_GATEOP,GLC_GATEOP,OLD_GATEOP FPV1Ma_SDG_V7.dss FPV1Ma_export.csv
```
