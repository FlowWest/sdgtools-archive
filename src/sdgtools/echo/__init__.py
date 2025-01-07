from pathlib import Path
from typing import List
import pandas as pd


def read_echo_file(file: Path, table_names: List[str] | str):
    """
    Read in echo file and return set of data configs used for post-processing
    """
    if type(table_names) == str:
        table_names = [table_names]

    out = {}

    with open(file, "r") as f:
        all_lines = f.readlines()

    for start_num, line in enumerate(all_lines, start=1):
        key = line.strip().lower()
        if key in table_names:
            for end_num, next_line in enumerate(
                all_lines[start_num:], start=start_num + 1
            ):
                if next_line.strip().lower() == "end":
                    out[key] = (start_num + 1, end_num - 1)
                    break

    data = {}
    for k, v in out.items():
        raw_rows = [r.split() for r in all_lines[v[0] - 1 : v[1]]]
        rows = [[r.strip() for r in d] for d in raw_rows]
        data[k] = pd.DataFrame(rows[1:], columns=rows[0])

    return data
