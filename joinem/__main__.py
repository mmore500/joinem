import argparse
import logging
import os
import sys
import typing
import warnings

import polars as pl
from tqdm import tqdm

__version__ = "0.3.2"


def get_scanner(filepath: str) -> typing.Callable:
    try:
        ext = os.path.splitext(filepath.replace("csv.gz", "csvgz"))[1]
        return {
            ".csv": pl.scan_csv,
            ".csvgz": pl.read_csv,  # gz scan not yet supported by polars
            ".csv.gz": pl.read_csv,  # gz scan not yet supported by polars
            ".fea": pl.read_ipc,  # scan not yet supported by polars
            ".feather": pl.read_ipc,  # scan not yet supported by polars
            ".json": pl.read_ndjson,  # scan not yet supported by polars
            ".parquet": pl.scan_parquet,
            ".pqt": pl.scan_parquet,
        }[ext or f".{filepath}"]
    except KeyError:
        raise ValueError(f"Unknown file type for {filepath}, ext={ext}")


def get_reader(filepath: str) -> typing.Callable:
    try:
        ext = os.path.splitext(filepath.replace("csv.gz", "csvgz"))[1]
        return {
            ".csv": pl.read_csv,
            ".csvgz": pl.read_csv,
            ".csv.gz": pl.read_csv,
            ".fea": pl.read_ipc,
            ".feather": pl.read_ipc,
            ".json": pl.read_ndjson,
            ".parquet": pl.read_parquet,
            ".pqt": pl.read_parquet,
        }[ext or f".{filepath}"]
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
        }[ext or f".{filepath}"]
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
            "find path/to/ -name '*.csv' | python3 -m joinem out.csv"
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
        "--stdin",
        action="store_true",
        default=False,
        help="Read data from stdin",
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
    parser.add_argument(
        "--input-filetype",
        action="store",
        help=(
            "Filetype of input. Otherwise, inferred. "
            "Example: csv, parquet, json, feather"
        ),
        type=str,
    )
    parser.add_argument(
        "--output-filetype",
        action="store",
        help="Filetype of output. Otherwise, inferred. Example: csv, parquet",
        type=str,
    )
    args = parser.parse_args()

    # silence warning...
    warnings.filterwarnings("ignore", message="Polars found a filename")
    # UserWarning: Polars found a filename. Ensure you pass a path to the
    # file instead of a python file object when possible for best
    # performance.
    lazy_frames = (
        [get_scanner, get_reader][args.stdin](
            filepath if args.input_filetype is None else args.input_filetype
        )([filepath, sys.stdin.buffer][args.stdin])
        .with_columns(
            *(
                eval_column(with_column, filepath)
                for with_column in args.with_columns
            )
        )
        .lazy()
        for filepath in [
            map(str.strip, sys.stdin),
            ("stdin",),
        ][args.stdin]
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
    get_sink(
        args.output_file
        if args.output_filetype is None
        else args.output_filetype
    )(result, args.output_file)


if __name__ == "__main__":
    main()
