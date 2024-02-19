#!/usr/bin/env python

import os

import pandas as pd

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    data_empty_x = pd.DataFrame(columns=["a", "b"]).reset_index(drop=True)
    data_empty_y = pd.DataFrame(columns=["b", "c"]).reset_index(drop=True)

    data_x = pd.DataFrame({"a": [1, 2], "b": [2.0, 3.0]}).reset_index(
        drop=True
    )
    data_y = pd.DataFrame({"c": [1, 2], "b": [2.0, 3.0]}).reset_index(
        drop=True
    )

    for ext, writer, opts in [
        (".csv", pd.DataFrame.to_csv, {"index": False}),
        (".fea", pd.DataFrame.to_feather, {}),
        (".feather", pd.DataFrame.to_feather, {}),
        (".json", pd.DataFrame.to_json, {}),
        (".parquet", pd.DataFrame.to_parquet, {"index": False}),
        (".pqt", pd.DataFrame.to_parquet, {"index": False}),
    ]:
        for i in range(3):
            writer(data_empty_x, f"assets/empty_x.{i}{ext}", **opts)
            writer(data_empty_y, f"assets/empty_y.{i}{ext}", **opts)
            writer(data_x, f"assets/x.{i}{ext}", **opts)
            writer(data_y, f"assets/y.{i}{ext}", **opts)
