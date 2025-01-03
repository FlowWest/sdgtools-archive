from pathlib import Path


def read_echo(file: Path):
    """
    Read in echo file and return set of data configs used for post-proessing
    """
    line_idx = {"gate_weir_device": [-1, -1]}

    # find line numbers for all key tables in the echo file
    with open(file, "r") as f:
        for start_num, line in enumerate(f, start=1):
            if line.strip().lower() in line_idx:
                for end_num, next_line in enumerate(f, start=start_num):
                    if next_line.strip().lower() == "end":
                        line_idx[line.lower()] = [start_num, end_num]
