import argparse
import contextlib
import functools
import inspect
import logging
import os
import subprocess
import sys
import typing
import warnings

import polars as pl


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


def _get_ext(filepath: str) -> str:
    # e.g., "dir/x.0.csv.gz" -> ".csv.gz", "csv.gz" -> ".csv.gz", "csv" -> ".csv"
    uncompressed = filepath.removesuffix(".gz").removesuffix(".zst")
    compression = filepath[len(uncompressed) :]
    root, ext = os.path.splitext(uncompressed)
    return (ext or f".{root}") + compression


def _make_compressed_writer(
    writer: typing.Callable, compression: str
) -> typing.Callable:
    if "compression" not in inspect.signature(writer).parameters:

        def unsupported(*args, **kwargs) -> typing.NoReturn:
            raise ValueError(
                f"{compression} compression for {writer.__qualname__} "
                f"is unsupported in polars {pl.__version__}",
            )

        return unsupported

    # check_extension=False allows writing to /dev/stdout and /dev/fd/*
    return functools.partial(
        writer, compression=compression, check_extension=False
    )


def _write_show(
    frame: typing.Union[pl.DataFrame, pl.LazyFrame],
    path: str,
    **show_kwargs,
) -> None:
    with open(path, "w") as f, contextlib.redirect_stdout(f):
        try:
            frame.show(**show_kwargs)
        except ValueError:
            frame.lazy().collect().show(**show_kwargs)


def _get_scanner(filepath: str) -> typing.Callable:
    try:
        ext = _get_ext(filepath)
        return {
            ".csv": pl.scan_csv,
            ".csvgz": pl.scan_csv,
            ".csv.gz": pl.scan_csv,
            ".csv.zst": pl.scan_csv,
            ".fea": pl.read_ipc,  # scan not yet supported by polars
            ".feather": pl.read_ipc,  # scan not yet supported by polars
            ".json": pl.scan_ndjson,
            ".json.gz": pl.scan_ndjson,
            ".json.zst": pl.scan_ndjson,
            ".jsonl": pl.scan_ndjson,
            ".jsonl.gz": pl.scan_ndjson,
            ".jsonl.zst": pl.scan_ndjson,
            ".ndjson": pl.scan_ndjson,
            ".ndjson.gz": pl.scan_ndjson,
            ".ndjson.zst": pl.scan_ndjson,
            ".parquet": pl.scan_parquet,
            ".pqt": pl.scan_parquet,
        }[ext]
    except KeyError:
        raise ValueError(f"Unknown file type for {filepath}, ext={ext}")


def _get_reader(filepath: str) -> typing.Callable:
    try:
        ext = _get_ext(filepath)
        return {
            ".csv": pl.read_csv,
            ".csvgz": pl.read_csv,
            ".csv.gz": pl.read_csv,
            ".csv.zst": pl.read_csv,
            ".fea": pl.read_ipc,
            ".feather": pl.read_ipc,
            ".json": pl.read_ndjson,
            ".json.gz": pl.read_ndjson,
            ".json.zst": pl.read_ndjson,
            ".jsonl": pl.read_ndjson,
            ".jsonl.gz": pl.read_ndjson,
            ".jsonl.zst": pl.read_ndjson,
            ".ndjson": pl.read_ndjson,
            ".ndjson.gz": pl.read_ndjson,
            ".ndjson.zst": pl.read_ndjson,
            ".parquet": pl.read_parquet,
            ".pqt": pl.read_parquet,
        }[ext]
    except KeyError:
        raise ValueError(f"Unknown file type for {filepath}, ext={ext}")


def _get_sink(filepath: str) -> typing.Callable:
    ext = _get_ext(filepath)
    try:
        return {
            ".csv": pl.LazyFrame.sink_csv,
            ".csv.gz": _make_compressed_writer(pl.LazyFrame.sink_csv, "gzip"),
            ".csv.zst": _make_compressed_writer(pl.LazyFrame.sink_csv, "zstd"),
            ".fea": pl.LazyFrame.sink_ipc,
            ".feather": pl.LazyFrame.sink_ipc,
            ".json": pl.LazyFrame.sink_ndjson,
            ".json.gz": _make_compressed_writer(
                pl.LazyFrame.sink_ndjson, "gzip"
            ),
            ".json.zst": _make_compressed_writer(
                pl.LazyFrame.sink_ndjson, "zstd"
            ),
            ".jsonl": pl.LazyFrame.sink_ndjson,
            ".jsonl.gz": _make_compressed_writer(
                pl.LazyFrame.sink_ndjson, "gzip"
            ),
            ".jsonl.zst": _make_compressed_writer(
                pl.LazyFrame.sink_ndjson, "zstd"
            ),
            ".ndjson": pl.LazyFrame.sink_ndjson,
            ".ndjson.gz": _make_compressed_writer(
                pl.LazyFrame.sink_ndjson, "gzip"
            ),
            ".ndjson.zst": _make_compressed_writer(
                pl.LazyFrame.sink_ndjson, "zstd"
            ),
            ".parquet": pl.LazyFrame.sink_parquet,
            ".pqt": pl.LazyFrame.sink_parquet,
            ".show": _write_show,
        }[ext]
    except KeyError:
        raise ValueError(f"Unknown file type for {filepath}, ext={ext}")


def _get_write(filepath: str) -> typing.Callable:
    ext = _get_ext(filepath)
    try:
        return {
            ".csv": pl.DataFrame.write_csv,
            ".csv.gz": _make_compressed_writer(pl.DataFrame.write_csv, "gzip"),
            ".csv.zst": _make_compressed_writer(
                pl.DataFrame.write_csv, "zstd"
            ),
            ".fea": pl.DataFrame.write_ipc,
            ".feather": pl.DataFrame.write_ipc,
            ".json": pl.DataFrame.write_ndjson,
            ".json.gz": _make_compressed_writer(
                pl.DataFrame.write_ndjson, "gzip"
            ),
            ".json.zst": _make_compressed_writer(
                pl.DataFrame.write_ndjson, "zstd"
            ),
            ".jsonl": pl.DataFrame.write_ndjson,
            ".jsonl.gz": _make_compressed_writer(
                pl.DataFrame.write_ndjson, "gzip"
            ),
            ".jsonl.zst": _make_compressed_writer(
                pl.DataFrame.write_ndjson, "zstd"
            ),
            ".ndjson": pl.DataFrame.write_ndjson,
            ".ndjson.gz": _make_compressed_writer(
                pl.DataFrame.write_ndjson, "gzip"
            ),
            ".ndjson.zst": _make_compressed_writer(
                pl.DataFrame.write_ndjson, "zstd"
            ),
            ".parquet": pl.DataFrame.write_parquet,
            ".pqt": pl.DataFrame.write_parquet,
            ".show": _write_show,
        }[ext]
    except KeyError:
        raise ValueError(f"Unknown file type for {filepath}, ext={ext}")


@contextlib.contextmanager
def _progress_pipe(filepath: str) -> typing.Iterator[str]:
    # pipe output through tqdm, which logs throughput and writes to file
    cmd = [sys.executable, "-m", "tqdm", "--bytes", "--mininterval", "5"]
    with open(filepath, "wb") as f, subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=f
    ) as proc:
        yield f"/dev/fd/{proc.stdin.fileno()}"


def _eval_init(init_eval: str) -> None:
    try:
        eval(init_eval)
    except Exception as e:
        logging.error(
            "Failed to parse init-eval expression `%s` error: %s",
            init_eval,
            e,
        )
        sys.exit(1)


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


def _eval_sample(sample: str) -> typing.Union[int, float, pl.Expr]:
    try:
        return eval(sample)
    except Exception as e:
        logging.error(
            "Failed to parse sample expression `%s` error: %s", sample, e
        )
        sys.exit(1)


def _eval_pipe(pipe: str, op: typing.Callable, flag: str) -> typing.Callable:
    try:
        return eval(pipe, {"pl": pl, "op": op})
    except Exception as e:
        logging.error(
            "Failed to parse %s expression `%s` error: %s", flag, pipe, e
        )
        sys.exit(1)


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
        help="Log bytes written and write throughput.",
    )
    _try_add_argument(
        parser,
        "--stdin",
        action="store_true",
        default=False,
        help="Read data from stdin.",
    )
    _try_add_argument(
        parser,
        "--drop",
        action="append",
        default=[],
        dest="drop",
        help=(
            "Column names to drop. "
            "Flag may be repeated to provide multiple column names."
        ),
        type=str,
    )
    _try_add_argument(
        parser,
        "--select",
        action="append",
        default=[],
        dest="select",
        help=(
            "Column names to select; otherwise, all columns are selected. "
            "Flag may be repeated to provide multiple column names."
        ),
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
            "Accepts an integer, a float (fraction of rows), "
            "or polars expression, with access to polars as `pl`. "
            "Sampling more rows than available raises an error; to sample "
            "up to a maximum, use an expression such as "
            r"""'pl.len().clip(upper_bound=100)'. """
            "Pass --seed for deterministic behavior."
        ),
        type=str,
    )
    _try_add_argument(
        parser,
        "--sample-stratify",
        action="append",
        default=[],
        dest="sample_stratify",
        help=(
            "Column name to stratify --sample by, so that the sample size "
            "applies to each distinct value (or combination of values, if "
            "repeated). To stratify by a derived value, add it as a column "
            "with --with-column and pass its alias. "
            "To sample up to a maximum per group, combine with a --sample "
            "expression such as "
            r"""'pl.len().clip(upper_bound=100)'."""
        ),
        type=str,
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
        "--init-eval",
        action="append",
        default=[],
        dest="init_evals",
        help=(
            "Expression to be evaluated once for initialization side "
            "effects before any data is processed, has access to polars "
            "as `pl`. Flag may be repeated to provide multiple "
            "expressions. Example: "
            r"""'pl.Config.set_engine_affinity(engine="streaming")'"""
        ),
        type=str,
    )
    _try_add_argument(
        parser,
        "--ipipe",
        default="op",
        dest="ipipe",
        help=(
            "Expression evaluating to a callable applied to each input "
            "frame, before concatenation, via `df.pipe(...)`. "
            "Has access to polars as `pl` and the default input dataframe "
            "op as `op`, which is a no-op for joinem itself but may do work "
            "when joinem is the backing CLI engine for another tool. "
            "Default: 'op'. Example: "
            r"""'lambda df: op(df).with_columns(pl.lit(1).alias("one"))'"""
        ),
        type=str,
    )
    _try_add_argument(
        parser,
        "--opipe",
        default="op",
        dest="opipe",
        help=(
            "Expression evaluating to a callable applied to the "
            "concatenated output frame via `df.pipe(...)`. "
            "Has access to polars as `pl` and the default output dataframe "
            "op as `op`, which is a no-op for joinem itself but may do work "
            "when joinem is the backing CLI engine for another tool. "
            "Default: 'op'. Example: "
            r"""'lambda df: op(df).unique()'"""
        ),
        type=str,
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
            "Flag may be repeated to provide multiple expressions. "
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
        help=(
            "Filetype of output. Otherwise, inferred. "
            "Example: csv, parquet. "
            "Use show to pretty print via DataFrame.show."
        ),
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
    for init_eval in args.init_evals:
        _eval_init(init_eval)

    if args.string_cache:
        try:
            pl.enable_string_cache()
        except AttributeError:
            logging.warning(
                "--string-cache is unsupported in polars %s, ignoring.",
                pl.__version__,
            )

    lazy_frames = (
        [_get_scanner, _get_reader][args.stdin or args.eager_read](
            filepath if args.input_filetype is None else args.input_filetype
        )(
            [filepath, sys.stdin.buffer][args.stdin],
            **_eval_kwargs(args.read_kwargs),
        )
        .with_columns(
            *(
                column
                for with_column in args.with_columns
                for column in _eval_column(f"[{with_column}]", filepath)
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
    ipipe = _eval_pipe(args.ipipe, input_dataframe_op, "--ipipe")
    lazy_frames = (df.pipe(ipipe).lazy() for df in lazy_frames)
    try:
        result = pl.concat(lazy_frames, how=args.how, rechunk=False)
    except ValueError as e:
        if "cannot concat empty list" in str(e):
            logging.warning("No input files provided.")
            sys.exit(0)
        logging.error("Failed to concatenate frames, error: %s", e)
        sys.exit(1)

    result = result.drop(args.drop)
    if args.select:
        result = result.select(args.select)
    if args.head is not None:
        result = result.head(args.head)
    if args.tail is not None:
        # tail is not yet fully supported by polars lazy API
        result = result.tail(args.tail).collect().lazy()
    if args.gather_every is not None:
        result = result.gather_every(args.gather_every)
    if args.sample is not None:
        sample = _eval_sample(args.sample)
        sampled = pl.struct(pl.all()).sample(
            **{"fraction" if isinstance(sample, float) else "n": sample},
            seed=args.seed,
        )
        if args.sample_stratify:
            # shuffle first so a shared seed picks different rows per group
            result = (
                result.collect()
                .sample(fraction=1, shuffle=True, seed=args.seed)
                .lazy()
            )
            sampled = sampled.over(
                *args.sample_stratify,
                mapping_strategy="explode",
            )
        result = result.select(sampled.alias("__joinem_sampled__")).unnest(
            "__joinem_sampled__"
        )
    if args.shuffle:
        result = result.collect().sample(shuffle=True, seed=args.seed).lazy()
    opipe = _eval_pipe(args.opipe, output_dataframe_op, "--opipe")
    result = result.pipe(opipe).lazy()

    if args.shrink_dtypes:
        result = (
            result.collect()
            .map_columns(pl.selectors.all(), pl.Series.shrink_dtype)
            .lazy()
        )

    progress = args.progress and os.path.isdir("/dev/fd")
    if args.progress and not progress:
        logging.warning("--progress requires /dev/fd, which is unavailable.")

    context = (
        _progress_pipe(args.output_file)
        if progress
        else contextlib.nullcontext(args.output_file)
    )
    with context as output_path:
        [_get_sink, _get_write][args.eager_write](
            args.output_file
            if args.output_filetype is None
            else args.output_filetype
        )(
            result.collect() if args.eager_write else result,
            output_path,
            **_eval_kwargs(args.write_kwargs),
        )
