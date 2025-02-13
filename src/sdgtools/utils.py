def concat_columns(df):
    columns = ["A", "B", "C", "F", "E", "D"]
    return df[columns].astype(str).agg("/".join, axis=1)


def add_node_and_param_cols(df):
    df_copy = df.copy()
    df_copy = df_copy.reset_index(names=["datetime"])
    col_to_sep = df_copy.columns[1]
    dss_parts = col_to_sep.split("/")
    df_copy["node"] = dss_parts[2]
    df_copy["param"] = dss_parts[3]
    df_copy = df_copy.rename(columns={col_to_sep: "value"})
    return df_copy
