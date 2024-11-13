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
- Supports CSV and parquet input files.
    - Due to current polars limitations, JSON and feather files are not supported.
    - Input formats may be mixed.
- Supports output to CSV, JSON, parquet, and feather file types.
- Allows mismatched columns and/or empty data files with `--how diagonal` and `--how diagonal_relaxed`.
- Provides a progress bar with `--progress`.
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

Use `--progress` to show a progress bar.
```
ls -1 path/{*.csv,*.pqt} | python3 -m joinem out.csv --progress
```

If file columns may mismatch, use `--how diagonal`.
```
find path/to/ -name '*.csv' | python3 -m joinem out.csv --how diagonal
```

If some files may be empty, use `--how diagonal_relaxed`.

To run via Singularity/Apptainer,
```
ls -1 *.csv | singularity run docker://ghcr.io/mmore500/joinem out.feather
```

Add literal value column to output.
```
ls -1 *.csv | python3 -m joinem out.csv --with-column 'pl.lit(2).alias("two")'
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

Read data from stdin and write data to stdout.
```
cat foo.csv | python3 -m joinem "/dev/stdout" --stdin --output-filetype csv --input-filetype csv
```

Advanced usage.
Write to parquet via stdout using `pv` to display progress, cast "myValue" column to categorical, and use lz4 for parquet compression.
```
ls -1 input/*.pqt | python3 -m joinem "/dev/stdout" --output-filetype pqt --with-column 'pl.col("myValue").cast(pl.Categorical)' --write-kwarg 'compression="lz4"' | pv > concat.pqt
```

## API

```
usage: __main__.py [-h] [--version] [--progress] [--stdin] [--eager-read]
                   [--eager-write] [--with-column WITH_COLUMNS]
                   [--string-cache]
                   [--how {vertical,horizontal,diagonal,diagonal_relaxed}]
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
  --progress            Show progress bar
  --stdin               Read data from stdin
  --eager-read          Use read_* instead of scan_*. Can improve performance
                        in some cases.
  --eager-write         Use write_* instead of sink_*. Can improve performance
                        in some cases.
  --filter FILTERS      Expression to be evaluated and passed to polars DataFrame.filter.
                        Example: 'pl.col("thing") == 0'
  --with-column WITH_COLUMNS
                        Expression to be evaluated to add a column, as access
                        to each datafile's filepath as `filepath` and polars
                        as `pl`. Example:
                        'pl.lit(filepath).str.replace(r".*?([^/]*)\.csv",
                        r"${1}").alias("filename stem")'
  --shrink-dtypes       Shrink numeric columns to the minimal required datatype.
  --string-cache        Enable Polars global string cache.
  --how {vertical,horizontal,diagonal,diagonal_relaxed}
                        How to concatenate frames. See
                        <https://docs.pola.rs/py-
                        polars/html/reference/api/polars.concat.html> for more
                        information.
  --input-filetype INPUT_FILETYPE
                        Filetype of input. Otherwise, inferred. Example: csv,
                        parquet, json, feather
  --output-filetype OUTPUT_FILETYPE
                        Filetype of output. Otherwise, inferred. Example: csv,
                        parquet
  --read-kwarg READ_KWARGS
                        Additional keyword arguments to pass to pl.read_* or
                        pl.scan_* call(s). Provide as 'key=value'. Specify
                        multiple kwargs by using this flag multiple times.
                        Arguments will be evaluated as Python expressions.
                        Example: 'infer_schema_length=None'
  --write-kwarg WRITE_KWARGS
                        Additional keyword arguments to pass to pl.write_* or
                        pl.sink_* call. Provide as 'key=value'. Specify
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
