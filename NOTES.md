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

__device-flow__ - represents flows at the gates
- GLC_FLOW_FISH
- MID_FLOW_FISH
- MID_FLOW_GATE
- OLD_FLOW_FISH
- OLD_FLOW_GATE
