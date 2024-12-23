import h5py
import pandas as pd


paths = {"output_channel_names": "hydro/input/output_channel"}

scenario_file_path = "FPV2Mb.h5"
h5_file = h5py.File(scenario_file_path)
output_channel_names = h5_file[paths.get("output_channel_names")]
channel_names = pd.DataFrame(output_channel_names[:])
channel_names = channel_names.astype(
    {
        "name": str,
        "chan_no": int,
        "distance": str,
        "variable": str,
        "interval": str,
        "period_op": str,
        "file": str,
    }
)


def get_output_channel_names(h5):
    output_channel_names = h5[paths.get("output_channel_names")]
    channel_names = pd.DataFrame(output_channel_names[:])
    channel_names = channel_names.astype(
        {
            "name": str,
            "chan_no": int,
            "distance": str,
            "variable": str,
            "interval": str,
            "period_op": str,
            "file": str,
        }
    )

    return channel_names


def get_channels_list(h5_path): ...
