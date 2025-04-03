import pandas as pd
from dataclasses import dataclass
from pathlib import Path
import os
import datetime
import pyhecdss
from pyhecdss.pyhecdss import DSSFile, get_matching_ts
from sdgtools.utils import add_node_and_param_cols

from typing import Any, Dict, List

from pandas.core.series import validate_bool_kwarg

param_to_unit = {"FLOW": "CFS", "STAGE": "FEET", "DEVICE-FLOW": "CFS"}


def read_echo_file(filepath: str):
    with open(filepath, "r") as f:
        lines = f.readlines()

    start_index = next(
        (i for i, line in enumerate(lines) if "GATE_WEIR_DEVICE" in line), None
    )
    end_index = next(
        (i for i, line in enumerate(lines) if "END" in line and i > start_index), None
    )

    if start_index is not None and end_index is not None:
        data_rows = lines[start_index + 1 : end_index]
        col_names = [
            "GATE_NAME",
            "DEVICE",
            "NDUPLICATE",
            "WIDTH",
            "ELEV",
            "HEIGHT",
            "CF_FROM_NODE",
            "CF_TO_NODE",
            "DEFAULT_OP",
        ]

        data = [line.split(maxsplit=len(col_names) - 1) for line in data_rows[1:]]
        df = pd.DataFrame(data, columns=col_names)
        df = df[df["DEVICE"] == "fish_passage"]
        return df


def concat_parts_to_path(item) -> Dict[str, str]:
    d = {
        "node": item.B,
        "param": item.C,
        "path": f"/{item.A}/{item.B}/{item.C}//{item.E}/{item.F}/",
    }
    return d


def make_regex_from_parts(A=None, B=None, C=None, D=None, E=None, F=None):
    parts = [A, B, C, D, E, F]
    return "/" + "/".join(x if x is not None else ".*" for x in parts) + "/"


def get_all_data_from_dsm2_dss(
    file: str, parts_regex: str | None = make_regex_from_parts()
) -> pd.DataFrame:
    all_paths = list(pyhecdss.get_matching_ts(file, parts_regex))
    if len(all_paths) == 0:
        return pd.DataFrame()
    data = [add_node_and_param_cols(all_paths[i].data) for i in range(len(all_paths))]
    concat_data = pd.concat(data)
    concat_data["unit"] = concat_data["param"].map(param_to_unit)
    return concat_data


def read_scenario_dir(
    sdg_path: str,
    hydro_path: str,
    echo_path: str,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Reads and processes scenario data from a directory containing DSS files.
    Matches corresponding hydro and SDG files based on naming patterns and returns their processed data.

    The function expects a specific directory structure where DSS files are stored in an 'output'
    subdirectory. It matches hydro and SDG files that share the same base name (e.g., 'scenario1_hydro.dss'
    with 'scenario1_SDG.dss').

    Parameters
    ----------
    dir : str
        Path to the main scenario directory. The function will look for files in a subdirectory
        named 'output'.
    v7_filter : str | None, optional
        If provided, only processes files containing this string (case-insensitive).
    """
    # first need to get a list of all scnarios in this folder
    scenario_path = Path(dir)
    output_path = scenario_path / "output"
    output_path_files = (
        list(output_path.iterdir())
        if v7_filter is None
        else [f for f in output_path.iterdir() if v7_filter.lower() in f.name.lower()]
    )
    hydro_files = [
        f for f in output_path_files if "hydro" in f.name.lower() and f.suffix == ".dss"
    ]

    sdg_files = [
        f for f in output_path_files if "sdg" in f.name.lower() and f.suffix == ".dss"
    ]

    echo_files_in_path = [
        f for f in output_path_files if "echo" in f.name.lower() and f.suffix == ".inp"
    ]

    if len(hydro_files) != len(sdg_files):
        raise ValueError(f"the number of hydro files and sdg must be equal")

    matches = list()
    for f in hydro_files:
        for s in sdg_files:
            f_split = f.stem.split("_")
            s_split = s.stem.split("_")
            if len(f_split) == len(s_split) and f_split[0] == s_split[0]:
                print(f"mathing:\t\t{f} <----> {s}")
                matches.append((f, s))

    data = {}
    try:
        for match in matches:
            data[str(match[0])] = {
                "hydro": get_all_data_from_dsm2_dss(HecDss(str(match[0]))),
                "sdg": get_all_data_from_dsm2_dss(HecDss(str(match[1]))),
            }

    except Exception as e:
        print(e)

    return data
