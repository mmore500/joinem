import argparse
import logging
import os
import sys
import typing
import warnings

import polars as pl
from tqdm import tqdm


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


def get_write(filepath: str) -> typing.Callable:
    try:
        ext = os.path.splitext(filepath)[1]
        return {
            ".csv": pl.DataFrame.write_csv,
            ".fea": pl.DataFrame.write_ipc,
            ".feather": pl.DataFrame.write_ipc,
            ".json": pl.DataFrame.write_ndjson,
            ".parquet": pl.DataFrame.write_parquet,
            ".pqt": pl.DataFrame.write_parquet,
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


def eval_filter(with_column: str) -> pl.Expr:
    try:
        return eval(with_column)
    except Exception as e:
        logging.error(
            "Failed to parse filter expression `%s`"
            " error: %s",
            with_column,
            e,
        )


def eval_kwargs(kwargs_list: typing.List[str]) -> typing.Dict:
    to_eval = f"dict({','.join(kwargs_list)})"
    try:
        return eval(to_eval)
    except Exception as e:
        logging.error(
            "Failed to parse kwarg expressions `%s` via `%s`"
            " error: %s",
            kwargs_list,
            to_eval,
            e,
        )
        sys.exit(1)


def dataframe_cli(
    *,
    description: str,
    module: str,
    version: str,
    input_dataframe_op: typing.Callable = lambda x: x,
    output_dataframe_op: typing.Callable = lambda x: x,
) -> None:
    parser = argparse.ArgumentParser(
        description=description,
        epilog=(
            "Provide input filepaths via stdin. Example: "
            f"find path/to/ -name '*.csv' | python3 -m {module} out.csv"
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"v{version}"
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
        "--eager-read",
        action="store_true",
        default=False,
        help=(
            "Use read_* instead of scan_*. "
            "Can improve performance in some cases."
        ),
    )
    parser.add_argument(
        "--eager-write",
        action="store_true",
        default=False,
        help=(
            "Use write_* instead of sink_*. "
            "Can improve performance in some cases."
        ),
    )
    parser.add_argument(
        "--filter",
        action="append",
        default=[],
        dest="filters",
        help=(
            "Expression to be evaluated and passed to polars DataFrame.filter. "
            "Example: "
            r"""'pl.col("thing") == 0'"""
        ),
        type=str,
    )
    parser.add_argument(
        "--with-column",
        action="append",
        default=[],
        dest="with_columns",
        help=(
            "Expression to be evaluated to add a column, has access to each "
            "datafile's filepath as `filepath` and polars as `pl`. "
            "Example: "
            r"""'pl.lit(filepath).str.replace(r".*?([^/]*)\.csv", r"${1}").alias("filename stem")'"""
        ),
        type=str,
    )
    parser.add_argument(
        "--shrink-dtypes",
        action="store_true",
        help="Shrink numeric columns to the minimal required datatype.",
        default=False,
    )
    parser.add_argument(
        "--string-cache",
        action="store_true",
        help="Enable Polars global string cache.",
        default=False,
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
    parser.add_argument(
        "--read-kwarg",
        action="append",
        default=[],
        dest="read_kwargs",
        help=(
            "Additional keyword arguments to pass to pl.read_* or pl.scan_* call(s). "
            "Provide as 'key=value'. "
            "Specify multiple kwargs by using this flag multiple times. "
            "Arguments will be evaluated as Python expressions. "
            "Example: 'infer_schema_length=None'"
        ),
        type=str,
    )
    parser.add_argument(
        "--write-kwarg",
        action="append",
        default=[],
        dest="write_kwargs",
        help=(
            "Additional keyword arguments to pass to pl.write_* or pl.sink_* call. "
            "Provide as 'key=value'. "
            "Specify multiple kwargs by using this flag multiple times. "
            "Arguments will be evaluated as Python expressions. "
            """Example: 'compression="lz4"'"""
        ),
        type=str,
    )
    args = parser.parse_args()

    # silence warning...
    warnings.filterwarnings("ignore", message="Polars found a filename")
    # UserWarning: Polars found a filename. Ensure you pass a path to the
    # file instead of a python file object when possible for best
    # performance.
    if args.string_cache:
        pl.enable_string_cache()

    lazy_frames = (
        [get_scanner, get_reader][args.stdin or args.eager_read](
            filepath if args.input_filetype is None else args.input_filetype
        )(
            [filepath, sys.stdin.buffer][args.stdin],
            **eval_kwargs(args.read_kwargs),
        )
        .with_columns(
            *(
                eval_column(with_column, filepath)
                for with_column in args.with_columns
            ),
        )
        .filter(
            *(
                eval_filter(filter)
                for filter in args.filters or ("True",)
            ),
        )
        .lazy()
        for filepath in [
            map(str.strip, sys.stdin),
            ("stdin",),
        ][args.stdin]
    )
    lazy_frames = map(lambda x: x.lazy(), map(input_dataframe_op, lazy_frames))
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

    result = output_dataframe_op(result).lazy()

    if args.shrink_dtypes:
        # shrink_dtype is not yet fully supported by polars lazy API
        result = result.select(pl.all().shrink_dtype()).collect().lazy()

    [get_sink, get_write][args.eager_write](
        args.output_file
        if args.output_filetype is None
        else args.output_filetype
    )(
        result.collect() if args.eager_write else result,
        args.output_file,
        **eval_kwargs(args.write_kwargs),
    )
