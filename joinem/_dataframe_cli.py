import argparse
import logging
import os
import sys
import typing
import warnings

import polars as pl
from tqdm import tqdm


def dataframe_cli(
    *,
    description: str,
    module: str,
    version: str,
    input_dataframe_op: typing.Callable = lambda x: x,
    output_dataframe_op: typing.Callable = lambda x: x,
) -> None:
    parser = argparse.ArgumentParser(description=description)
    parser = _add_parser_base(
        parser=parser, dfcli_module=module, dfcli_version=version
    )
    return _run_dataframe_cli(
        base_parser=parser,
        input_dataframe_op=input_dataframe_op,
        output_dataframe_op=output_dataframe_op,
    )


def _get_scanner(filepath: str) -> typing.Callable:
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


def _get_reader(filepath: str) -> typing.Callable:
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


def _get_sink(filepath: str) -> typing.Callable:
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


def _get_write(filepath: str) -> typing.Callable:
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


def _eval_column(with_column: str, filepath: str) -> pl.Expr:
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


def _eval_filter(with_column: str) -> pl.Expr:
    try:
        return eval(with_column)
    except Exception as e:
        logging.error(
            "Failed to parse filter expression `%s`" " error: %s",
            with_column,
            e,
        )


def _eval_kwargs(kwargs_list: typing.List[str]) -> typing.Dict:
    to_eval = f"dict({','.join(kwargs_list)})"
    try:
        return eval(to_eval)
    except Exception as e:
        logging.error(
            "Failed to parse kwarg expressions `%s` via `%s`" " error: %s",
            kwargs_list,
            to_eval,
            e,
        )
        sys.exit(1)


def _add_parser_base(
    *,
    parser: argparse.ArgumentParser,
    dfcli_module: str,
    dfcli_version: str,
) -> argparse.ArgumentParser:
    parser.epilog = (
        "Provide input filepaths via stdin. Example: "
        f"find path/to/ -name '*.csv' | python3 -m {dfcli_module} out.csv"
    )
    parser.add_argument(
        "--version", action="version", version=f"v{dfcli_version}"
    )
    return parser


def _add_parser_core(
    *,
    overridden_arguments: typing.Literal["error", "ignore", "warn"] = "error",
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    if not parser.add_help:
        parser.add_argument(
            "--help",
            "-h",
            action="help",
            help="show this help message and exit",
        )

    def _try_add_argument(
        parser: argparse.ArgumentParser,
        /,
        *args: list,
        **kwargs: dict,
    ) -> None:
        try:
            parser.add_argument(*args, **kwargs)
        except argparse.ArgumentError as e:
            if overridden_arguments == "error":
                raise e
            elif overridden_arguments == "ignore":
                pass
            elif overridden_arguments == "warn":
                warnings.warn(e)
            else:
                raise ValueError(
                    f"{overridden_arguments=} must be one of "
                    "'error', 'ignore', or 'warn'.",
                )

    _try_add_argument(parser, "output_file", type=str, help="Output file name")
    _try_add_argument(
        parser,
        "--progress",
        action="store_true",
        help="Show progress bar",
    )
    _try_add_argument(
        parser,
        "--stdin",
        action="store_true",
        default=False,
        help="Read data from stdin",
    )
    _try_add_argument(
        parser,
        "--drop",
        action="append",
        default=[],
        dest="drop",
        help="Columns to drop.",
        type=str,
    )
    _try_add_argument(
        parser,
        "--eager-read",
        action="store_true",
        default=False,
        help=(
            "Use read_* instead of scan_*. "
            "Can improve performance in some cases."
        ),
    )
    _try_add_argument(
        parser,
        "--eager-write",
        action="store_true",
        default=False,
        help=(
            "Use write_* instead of sink_*. "
            "Can improve performance in some cases."
        ),
    )
    _try_add_argument(
        parser,
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
    _try_add_argument(
        parser,
        "--head",
        default=None,
        dest="head",
        help=("Number of rows to include in output, counting from front. "),
        type=int,
    )
    _try_add_argument(
        parser,
        "--tail",
        default=None,
        dest="tail",
        help=("Number of rows to include in output, counting from back. "),
        type=int,
    )
    _try_add_argument(
        parser,
        "--gather-every",
        default=None,
        dest="gather_every",
        help=("Take every nth row. "),
        type=int,
    )
    _try_add_argument(
        parser,
        "--sample",
        default=None,
        dest="sample",
        help=(
            "Number of rows to include in output, sampled uniformly. "
            "Pass --seed for deterministic behavior."
        ),
        type=int,
    )
    _try_add_argument(
        parser,
        "--shuffle",
        action="store_true",
        dest="shuffle",
        help=(
            "Should output be shuffled? "
            "Pass --seed for deterministic behavior."
        ),
        default=False,
    )
    _try_add_argument(
        parser,
        "--seed",
        default=None,
        dest="seed",
        help="Integer seed for deterministic behavior.",
        type=int,
    )
    _try_add_argument(
        parser,
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
    _try_add_argument(
        parser,
        "--shrink-dtypes",
        action="store_true",
        help="Shrink numeric columns to the minimal required datatype.",
        default=False,
    )
    _try_add_argument(
        parser,
        "--string-cache",
        action="store_true",
        help="Enable Polars global string cache.",
        default=False,
    )
    _try_add_argument(
        parser,
        "--how",
        choices=[
            "vertical",
            "vertical_relaxed",
            "diagonal",
            "diagonal_relaxed",
            "horizontal",
            "align",
            "align_full",
            "align_inner",
            "align_left",
            "align_right",
        ],
        default="vertical",
        help="How to concatenate frames. See <https://docs.pola.rs/py-polars/html/reference/api/polars.concat.html> for more information.",
    )
    _try_add_argument(
        parser,
        "--input-filetype",
        action="store",
        help=(
            "Filetype of input. Otherwise, inferred. "
            "Example: csv, parquet, json, feather"
        ),
        type=str,
    )
    _try_add_argument(
        parser,
        "--output-filetype",
        action="store",
        help="Filetype of output. Otherwise, inferred. Example: csv, parquet",
        type=str,
    )
    _try_add_argument(
        parser,
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
    _try_add_argument(
        parser,
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
    return parser


def _run_dataframe_cli(
    *,
    base_parser: argparse.ArgumentParser,
    input_dataframe_op: typing.Callable = lambda x: x,
    output_dataframe_op: typing.Callable = lambda x: x,
    overridden_arguments: typing.Literal["error", "ignore", "warn"] = "error",
) -> None:

    parser = _add_parser_core(
        overridden_arguments=overridden_arguments,
        parser=base_parser,
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
        [_get_scanner, _get_reader][args.stdin or args.eager_read](
            filepath if args.input_filetype is None else args.input_filetype
        )(
            [filepath, sys.stdin.buffer][args.stdin],
            **_eval_kwargs(args.read_kwargs),
        )
        .with_columns(
            *(
                _eval_column(with_column, filepath)
                for with_column in args.with_columns
            ),
        )
        .filter(
            *(_eval_filter(filter) for filter in args.filters or ("True",)),
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

    result = result.drop(args.drop)
    if args.head is not None:
        result = result.head(args.head)
    if args.tail is not None:
        # tail is not yet fully supported by polars lazy API
        result = result.tail(args.tail).collect().lazy()
    if args.gather_every is not None:
        result = result.gather_every(args.gather_every)
    if args.sample is not None:
        result = result.collect().sample(n=args.sample, seed=args.seed).lazy()
    if args.shuffle:
        result = result.collect().sample(shuffle=True, seed=args.seed).lazy()
    result = output_dataframe_op(result).lazy()

    if args.shrink_dtypes:
        # shrink_dtype is not yet fully supported by polars lazy API
        result = result.select(pl.all().shrink_dtype()).collect().lazy()

    [_get_sink, _get_write][args.eager_write](
        args.output_file
        if args.output_filetype is None
        else args.output_filetype
    )(
        result.collect() if args.eager_write else result,
        args.output_file,
        **_eval_kwargs(args.write_kwargs),
    )
