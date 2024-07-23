import argparse
import logging
import os
import sys
import typing

import polars as pl
from tqdm import tqdm

__version__ = "0.2.1"


def get_scanner(filepath: str) -> typing.Callable:
    try:
        ext = os.path.splitext(filepath)[1]
        return {
            ".csv": pl.scan_csv,
            # ".fea": pl.scan_ipc,  # not yet supported by polars
            # ".feather": pl.scan_ipc,  # not yet supported by polars
            # ".json": pl.scan_ndjson,  # not yet supported by polars
            ".parquet": pl.scan_parquet,
            ".pqt": pl.scan_parquet,
        }[ext]
    except KeyError:
        raise ValueError(f"Unknown file type for {filepath}, ext={ext}")


def get_sink(filepath: str) -> typing.Callable:
    try:
        ext = os.path.splitext(filepath)[1]
        return {
            ".csv": pl.LazyFrame.sink_csv,
            ".fea": pl.LazyFrame.sink_ipc,
            ".feather": pl.LazyFrame.sink_ipc,
            ".json": pl.LazyFrame.sink_ndjson,
            ".parquet": pl.LazyFrame.sink_parquet,
            ".pqt": pl.LazyFrame.sink_parquet,
        }[ext]
    except KeyError:
        raise ValueError(f"Unknown file type for {filepath}, ext={ext}")


def eval_column(with_column: str, filepath: str) -> pl.Expr:
    try:
        return eval(with_column)
    except Exception as e:
        logging.error(
            "Failed to parse with_column expression `%s` for filepath `%s`"
            " error: %s",
            with_column,
            filepath,
            e,
        )
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Concatenate CSV and/or parquet tabular data files.",
        epilog=(
            "Provide input filepaths via stdin. Example: "
            "find path/to/ -name '*.csv' | python3 -m joinem -o out.csv"
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"joinem v{__version__}"
    )
    parser.add_argument("output_file", type=str, help="Output file name")
    parser.add_argument(
        "--progress", action="store_true", help="Show progress bar"
    )
    parser.add_argument(
        "--with-column",
        action="append",
        default=[],
        dest="with_columns",
        help=(
            "Expression to be evaluated to add a column, as access to each "
            "datafile's filepath as `filepath` and polars as `pl`. "
            "Example: "
            r"""'pl.lit(filepath).str.replace(r".*?([^/]*)\.csv", r"${1}").alias("filename stem")'"""
        ),
        type=str,
    )
    parser.add_argument(
        "--how",
        choices=["vertical", "horizontal", "diagonal", "diagonal_relaxed"],
        default="vertical",
        help="How to concatenate frames. See <https://docs.pola.rs/py-polars/html/reference/api/polars.concat.html> for more information.",
    )
    args = parser.parse_args()

    lazy_frames = (
        get_scanner(filepath)(filepath).with_columns(
            *(
                eval_column(with_column, filepath)
                for with_column in args.with_columns
            )
        )
        for filepath in map(str.strip, sys.stdin)
    )
    if args.progress:
        lazy_frames = tqdm(lazy_frames)

    try:
        result = pl.concat(lazy_frames, how=args.how, rechunk=False)
    except ValueError as e:
        if "cannot concat empty list" in str(e):
            logging.warning("No input files provided.")
            sys.exit(0)
        logging.error("Failed to concatenate frames, error: %s", e)
        sys.exit(1)
    get_sink(args.output_file)(result, args.output_file)


if __name__ == "__main__":
    main()
