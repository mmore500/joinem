[
![PyPi](https://img.shields.io/pypi/v/joinem.svg?)
](https://pypi.python.org/pypi/joinem)
[
![CI](https://github.com/mmore500/joinem/actions/workflows/ci.yaml/badge.svg)
](https://github.com/mmore500/joinem/actions)
[
![GitHub stars](https://img.shields.io/github/stars/mmore500/joinem.svg?style=round-square&logo=github&label=Stars&logoColor=white)](https://github.com/mmore500/joinem)
[![DOI](https://zenodo.org/badge/760045369.svg)](https://zenodo.org/doi/10.5281/zenodo.10701182)

**_joinem_** provides a CLI for fast, flexbile concatenation of tabular data using [polars](https://pola.rs/)

- Free software: MIT license
- Repository: <https://github.com/mmore500/joinem>
- Documentation: <https://github.com/mmore500/joinem/blob/master/README.md>

## Install

`python3 -m pip install joinem`

## Features

- Lazily streams I/O to expeditiously handle numerous large files.
- Supports CSV, JSON, parquet, and feather input files.
    - JSON input must be newline-delimited (`.json`, `.jsonl`, or `.ndjson`).
    - Compressed input is supported: `.csv.gz`, `.csv.zst`, `.json.gz`, `.json.zst`, `.jsonl.gz`, `.jsonl.zst`, `.ndjson.gz`, and `.ndjson.zst`.
    - Feather files are read eagerly rather than streamed.
    - Input formats may be mixed.
- Supports CSV, JSON, parquet, and feather output files.
    - JSON output is newline-delimited (`.json`, `.jsonl`, or `.ndjson`).
    - Compressed output is supported: `.csv.gz`, `.csv.zst`, `.json.gz`, `.json.zst`, `.jsonl.gz`, `.jsonl.zst`, `.ndjson.gz`, and `.ndjson.zst` (requires a recent polars).
- Supports pretty-printed text output (`.show`), via polars `DataFrame.show`.
- Allows mismatched columns and/or empty data files with `--how diagonal` and `--how diagonal_relaxed`.
- Logs bytes written and write throughput with `--progress`.
- Add programatically-generated columns to output.

## Example Usage

Pass input filenames via stdin, one filename per line.
```
find path/to/*.parquet path/to/*.csv | python3 -m joinem out.parquet
```

Output file type is inferred from the extension of the output file name.
Supported output types are feather, JSON, parquet, and csv.
```
find -name '*.parquet' | python3 -m joinem out.json
```

If file columns may mismatch, use `--how diagonal`.
```
find path/to/ -name '*.csv' | python3 -m joinem out.csv --how diagonal
```

If some files may be empty, use `--how diagonal_relaxed`.

To run via Singularity/Apptainer,
```
ls -1 *.csv | singularity run docker://ghcr.io/mmore500/joinem:v0.16.0 out.feather
```

## Advanced Usage

Add literal value column to output.
```
ls -1 *.csv | python3 -m joinem out.csv --with-column 'pl.lit(2).alias("two")'
```

Cast a column to categorical in the output, shrink dtypes, and tune compression.
```
ls -1 *.csv | python3 -m joinem out.pqt \
  --with-column 'pl.col("uuid").cast(pl.Categorical)' --string-cache \
  --shrink-dtypes \
  --write-kwarg 'compression_level=10' --write-kwarg 'compression="zstd"'
```

Alias an existing column in the output.
```
ls -1 *.csv | python3 -m joinem out.csv --with-column 'pl.col("a").alias("a2")'
```

Apply regex on source datafile paths to create new column in output.
```
ls -1 path/to/*.csv | python3 -m joinem out.csv \
  --with-column 'pl.lit(filepath).str.replace(r".*?([^/]*)\.csv", r"${1}").alias("filename stem")'
```

Write gzip-compressed CSV output; compression is inferred from the output file extension.
```
ls -1 *.csv.gz | python3 -m joinem out.csv.gz
```

Read data from stdin and write data to stdout.
```
cat foo.csv | python3 -m joinem "/dev/stdout" --stdin \
  --output-filetype csv --input-filetype csv
```

Write to parquet via stdout using `pv` to display progress, cast "myValue" column to categorical, and use lz4 for parquet compression.
```
ls -1 input/*.pqt | python3 -m joinem "/dev/stdout" --output-filetype pqt \
  --with-column 'pl.col("myValue").cast(pl.Categorical)' \
  --write-kwarg 'compression="lz4"' \
  | pv > concat.pqt
```

Write pretty print to stdout.
Use `--write-kwarg` to pass `DataFrame.show` settings, e.g., to print all rows and columns.
```
ls -1 *.csv | python3 -m joinem "/dev/stdout" --output-filetype show \
  --write-kwarg 'limit=None' --write-kwarg 'tbl_cols=-1'
```

Use `--init-eval` to configure Polars before processing begins, e.g., to set `pl.Config.set_engine_affinity` to prefer the streaming engine.
```
ls -1 *.csv | python3 -m joinem out.pqt \
  --init-eval 'pl.Config.set_engine_affinity(engine="streaming")'
```

Use `--ipipe` and `--opipe` to transform each input frame (before concatenation) or the output frame via `df.pipe(...)`.
Expressions should evaluate to a callable and have access to polars as `pl`.
```
ls -1 *.csv | python3 -m joinem out.pqt \
  --ipipe 'lambda df: df.with_columns(pl.len().alias("n"))' \
  --opipe 'pl.LazyFrame.unique'
```

Combine `--opipe` with `show` to pretty print summary statistics to stdout.
```
ls -1 *.csv | python3 -m joinem "/dev/stdout" --output-filetype show \
  --opipe 'pl.LazyFrame.describe' \
  --write-kwarg 'limit=None' --write-kwarg 'tbl_cols=-1'
```

Similarly, pretty print a column-wise preview from `glimpse`.
```
ls -1 *.csv | python3 -m joinem "/dev/stdout" --output-filetype show \
  --opipe 'lambda df: df.collect().glimpse(return_type="frame")'
```

## API

```
usage: __main__.py [-h] [--version] [--progress] [--stdin] [--drop DROP]
                   [--select SELECT] [--eager-read] [--eager-write]
                   [--filter FILTERS] [--head HEAD] [--tail TAIL]
                   [--gather-every GATHER_EVERY] [--sample SAMPLE]
                   [--sample-stratify SAMPLE_STRATIFY] [--shuffle]
                   [--seed SEED] [--init-eval INIT_EVALS] [--ipipe IPIPE]
                   [--opipe OPIPE] [--with-column WITH_COLUMNS]
                   [--shrink-dtypes] [--string-cache]
                   [--how {vertical,vertical_relaxed,diagonal,diagonal_relaxed,horizontal,align,align_full,align_inner,align_left,align_right}]
                   [--input-filetype INPUT_FILETYPE]
                   [--output-filetype OUTPUT_FILETYPE]
                   [--read-kwarg READ_KWARGS] [--write-kwarg WRITE_KWARGS]
                   output_file

CLI for fast, flexbile concatenation of tabular data using Polars.

positional arguments:
  output_file           Output file name

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --progress            Log bytes written and write throughput.
  --stdin               Read data from stdin.
  --drop DROP           Column names to drop. Flag may be repeated to
                        provide multiple column names.
  --select SELECT       Column names to select; otherwise, all columns are
                        selected. Flag may be repeated to provide multiple
                        column names.
  --eager-read          Use read_* instead of scan_*. Can improve
                        performance in some cases.
  --eager-write         Use write_* instead of sink_*. Can improve
                        performance in some cases.
  --filter FILTERS      Expression to be evaluated and passed to polars
                        DataFrame.filter. Example: 'pl.col("thing") == 0'
  --head HEAD           Number of rows to include in output, counting from
                        front.
  --tail TAIL           Number of rows to include in output, counting from
                        back.
  --gather-every GATHER_EVERY
                        Take every nth row.
  --sample SAMPLE       Number of rows to include in output, sampled
                        uniformly. Accepts an integer, a float (fraction of
                        rows), or polars expression, with access to polars
                        as `pl`. Sampling more rows than available raises an
                        error; to sample up to a maximum, use an expression
                        such as 'pl.len().clip(upper_bound=100)'. Pass
                        --seed for deterministic behavior.
  --sample-stratify SAMPLE_STRATIFY
                        Column name to stratify --sample by, so that the
                        sample size applies to each distinct value (or
                        combination of values, if repeated). To stratify by
                        a derived value, add it as a column with --with-
                        column and pass its alias. To sample up to a maximum
                        per group, combine with a --sample expression such
                        as 'pl.len().clip(upper_bound=100)'.
  --shuffle             Should output be shuffled? Pass --seed for
                        deterministic behavior.
  --seed SEED           Integer seed for deterministic behavior.
  --init-eval INIT_EVALS
                        Expression to be evaluated once for initialization
                        side effects before any data is processed, has
                        access to polars as `pl`. Flag may be repeated to
                        provide multiple expressions. Example:
                        'pl.Config.set_engine_affinity(engine="streaming")'
  --ipipe IPIPE         Expression evaluating to a callable applied to each
                        input frame, before concatenation, via `df.pipe(...)`.
                        Has access to polars as `pl` and the default input
                        dataframe op as `op`, which is a no-op for joinem
                        itself but may do work when joinem is the backing CLI
                        engine for another tool. Default: 'op'. Example:
                        'lambda df:
                        op(df).with_columns(pl.lit(1).alias("one"))'
  --opipe OPIPE         Expression evaluating to a callable applied to the
                        concatenated output frame via `df.pipe(...)`. Has
                        access to polars as `pl` and the default output
                        dataframe op as `op`, which is a no-op for joinem
                        itself but may do work when joinem is the backing CLI
                        engine for another tool. Default: 'op'. Example:
                        'lambda df: op(df).unique()'
  --with-column WITH_COLUMNS
                        Expression to be evaluated to add a column, has
                        access to each datafile's filepath as `filepath` and
                        polars as `pl`. Flag may be repeated to provide
                        multiple expressions. Example:
                        'pl.lit(filepath).str.replace(r".*?([^/]*)\.csv",
                        r"${1}").alias("filename stem")'
  --shrink-dtypes       Shrink numeric columns to the minimal required
                        datatype.
  --string-cache        Enable Polars global string cache.
  --how {vertical,vertical_relaxed,diagonal,diagonal_relaxed,horizontal,align,align_full,align_inner,align_left,align_right}
                        How to concatenate frames. See
                        <https://docs.pola.rs/py-
                        polars/html/reference/api/polars.concat.html> for
                        more information.
  --input-filetype INPUT_FILETYPE
                        Filetype of input. Otherwise, inferred. Example:
                        csv, parquet, json, feather
  --output-filetype OUTPUT_FILETYPE
                        Filetype of output. Otherwise, inferred. Example:
                        csv, parquet. Use show to pretty print via
                        DataFrame.show.
  --read-kwarg READ_KWARGS
                        Additional keyword arguments to pass to pl.read_* or
                        pl.scan_* call(s). Provide as 'key=value'. Specify
                        multiple kwargs by using this flag multiple times.
                        Arguments will be evaluated as Python expressions.
                        Example: 'infer_schema_length=None'
  --write-kwarg WRITE_KWARGS
                        Additional keyword arguments to pass to pl.write_*
                        or pl.sink_* call. Provide as 'key=value'. Specify
                        multiple kwargs by using this flag multiple times.
                        Arguments will be evaluated as Python expressions.
                        Example: 'compression="lz4"'

Provide input filepaths via stdin. Example: find path/to/ -name '*.csv' |
python3 -m joinem out.csv
```

## Citing

If *joinem* contributes to a scholarly work, please cite it as

> Matthew Andres Moreno. (2024). mmore500/joinem. Zenodo. https://doi.org/10.5281/zenodo.10701182

```bibtex
@software{moreno2024joinem,
  author = {Matthew Andres Moreno},
  title = {mmore500/joinem},
  month = feb,
  year = 2024,
  publisher = {Zenodo},
  doi = {10.5281/zenodo.10701182},
  url = {https://doi.org/10.5281/zenodo.10701182}
}
```

And don't forget to leave a [star on GitHub](https://github.com/mmore500/joinem/stargazers)!
