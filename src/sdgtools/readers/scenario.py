from typing import Dict, Any
from .dss import read_dss, make_dss_regex_from_parts


import pandas as pd
import pyhecdss
from pydsm.input import read_input

SDG_ELEVATION_LIST = [
    "MID_GATE_UP",
    "MID_GATE_DOWN",
    "GLC_GATE_UP",
    "GLC_GATE_DOWN",
    "OLD_GATE_UP",
    "OLD_GATE_DOWN",
]

SDG_FLOW_LIST = [
    "GLC_FLOW_FISH",
    "MID_FLOW_FISH",
    "MID_FLOW_GATE",
    "OLD_FLOW_FISH",
    "OLD_FLOW_GATE",
]

SDG_GATE_OP_LIST = ["MID_GATEOP", "GLC_GATEOP", "OLD_GATEOP"]


HYDRO_STATION_NAMES = ["MHO", "DGL", "OLD"]


def read_scenario(
    scneario_name: str,
    sdg_path: str,
    hydro_path: str,
    echo_path: str,
) -> Dict[str, Any]:
    """
    Reads a colleciton of three files to compile a scenario
    """
    sdg_stage = read_dss(
        sdg_path, make_dss_regex_from_parts(B=SDG_ELEVATION_LIST, C="STAGE")
    )
    sdg_flow = read_dss(
        sdg_path, make_dss_regex_from_parts(B=SDG_FLOW_LIST, C="DEVICE-FLOW")
    )
    sdg_gate_ops = read_dss(
        sdg_path, make_dss_regex_from_parts(B=SDG_GATE_OP_LIST, C="ELEV")
    )

    hydro_data = read_dss(
        hydro_path, make_dss_regex_from_parts(B=HYDRO_STATION_NAMES, C="STAGE")
    )

    # echo file
    echo_data = read_input(echo_path)
    gate_weir_dev = echo_data["GATE_WEIR_DEVICE"]
    grantline_settings = gate_weir_dev[gate_weir_dev["GATE_NAME"] == "grantline_gate"]
    middle_r_settings = gate_weir_dev[gate_weir_dev["GATE_NAME"] == "middle_r_gate"]
    old_r_settings = gate_weir_dev[gate_weir_dev["GATE_NAME"] == "old_r_gate"]
    echo_settings = {
        "grantline": {
            "width": grantline_settings["WIDTH"].item(),
            "bottom_elevation": grantline_settings["ELEV"].item(),
            "c_from_node": grantline_settings["CF_FROM_NODE"].item(),
            "c_to_node": grantline_settings["CF_TO_NODE"].item(),
        },
        "middle_river": {
            "width": middle_r_settings["WIDTH"].item(),
            "bottom_elevation": middle_r_settings["ELEV"].item(),
            "c_from_node": middle_r_settings["CF_FROM_NODE"].item(),
            "c_to_node": middle_r_settings["CF_TO_NODE"].item(),
        },
        "old_river": {
            "width": old_r_settings["WIDTH"].item(),
            "bottom_elevation": old_r_settings["ELEV"].item(),
            "c_from_node": old_r_settings["CF_FROM_NODE"].item(),
            "c_to_node": old_r_settings["CF_TO_NODE"].item(),
        },
    }

    return {
        "sdg_stage": sdg_stage,
        "sdg_flow": sdg_flow,
        "sdg_gate_ops": sdg_gate_ops,
        "wl_compliance": hydro_data,
        "echo_config": echo_settings,
    }
