import pyhecdss
import pandas as pd


PARAM_TO_UNIT = {"flow": "CFS", "stage": "FEET", "device-flow": "CFS"}


def add_node_and_param_cols(df):
    df_copy = df.copy()
    df_copy = df_copy.reset_index(names=["datetime"])
    col_to_sep = df_copy.columns[1]
    dss_parts = col_to_sep.split("/")
    df_copy["node"] = dss_parts[2].lower()
    df_copy["param"] = dss_parts[3].lower()
    df_copy = df_copy.rename(columns={col_to_sep: "value"})
    return df_copy


def make_dss_regex_from_parts(A=None, B=None, C=None, D=None, E=None, F=None):
    def do_regex_or(part):
        if part is None:
            return ".*"
        if isinstance(part, (list, tuple)):
            # Join list elements with | and wrap in parentheses if there's more than one element
            processed = "|".join(str(x) for x in part)
            return f"({processed})" if len(part) > 1 else processed
        return str(part)

    parts = [A, B, C, D, E, F]
    return "/" + "/".join(do_regex_or(x) for x in parts) + "/"


def read_dss(
    file: str, parts_regex: str | None = make_dss_regex_from_parts()
) -> pd.DataFrame:
    all_paths = list(pyhecdss.get_matching_ts(file, parts_regex))
    if len(all_paths) == 0:
        return pd.DataFrame()
    data = [add_node_and_param_cols(all_paths[i].data) for i in range(len(all_paths))]
    concat_data = pd.concat(data)
    concat_data["unit"] = concat_data["param"].map(PARAM_TO_UNIT)
    concat_data = concat_data[["datetime", "node", "param", "value", "unit"]]
    return concat_data
