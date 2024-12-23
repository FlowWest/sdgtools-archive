import h5py
import pandas as pd

H5PATHS = {"output_channel_names": "hydro/input/output_channel"}


def get_output_channel_names(h5):
    output_channel_names = h5[H5PATHS.get("output_channel_names")]
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
