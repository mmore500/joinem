import argparse
import logging
import os
import sys
import typing

import polars as pl
from tqdm import tqdm

__version__ = "0.0.0"


def get_scanner(filename: str) -> typing.Callable:
    try:
        ext = os.path.splitext(filename)[1]
        return {
            ".csv": pl.scan_csv,
            # ".fea": pl.scan_ipc,  # not yet supported by polars
            # ".feather": pl.scan_ipc,  # not yet supported by polars
            # ".json": pl.scan_ndjson,  # not yet supported by polars
            ".parquet": pl.scan_parquet,
            ".pqt": pl.scan_parquet,
        }[ext]
    except KeyError:
        raise ValueError(f"Unknown file type for {filename}, ext={ext}")


def get_sink(filename: str) -> typing.Callable:
    try:
        ext = os.path.splitext(filename)[1]
        return {
            ".csv": pl.LazyFrame.sink_csv,
            ".fea": pl.LazyFrame.sink_ipc,
            ".feather": pl.LazyFrame.sink_ipc,
            ".json": pl.LazyFrame.sink_ndjson,
            ".parquet": pl.LazyFrame.sink_parquet,
            ".pqt": pl.LazyFrame.sink_parquet,
        }[ext]
    except KeyError:
        raise ValueError(f"Unknown file type for {filename}, ext={ext}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Concatenate CSV and/or parquet tabular data files.",
        epilog=(
            "Provide input filenames via stdin. Example: "
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
        "--how",
        choices=["vertical", "horizontal", "diagonal", "diagonal_relaxed"],
        default="vertical",
        help="How to concatenate frames. See <https://docs.pola.rs/py-polars/html/reference/api/polars.concat.html> for more information.",
    )
    args = parser.parse_args()

    lazy_frames = (
        get_scanner(filename)(filename)
        for filename in map(str.strip, sys.stdin)
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
