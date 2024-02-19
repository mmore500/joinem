[
![PyPi](https://img.shields.io/pypi/v/joinem.svg?)
](https://pypi.python.org/pypi/joinem)
[
![CI](https://github.com/mmore500/joinem/actions/workflows/ci.yaml/badge.svg)
](https://github.com/mmore500/joinem/actions)
[
![GitHub stars](https://img.shields.io/github/stars/mmore500/joinem.svg?style=round-square&logo=github&label=Stars&logoColor=white)](https://github.com/mmore500/joinem)

_joinem_ provides a CLI for fast, flexbile concatenation of tabular data using [polars](https://pola.rs/)

- Free software: MIT license
- Repository: <https://github.com/mmore500/joinem>

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

## API

```
usage: __main__.py [-h] [--version] [--progress]
                   [--how {vertical,horizontal,diagonal,diagonal_relaxed}]
                   output_file

Concatenate CSV and/or parquet tabular data files.

positional arguments:
  output_file           Output file name

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --progress            Show progress bar
  --how {vertical,horizontal,diagonal,diagonal_relaxed}
                        How to concatenate frames. See <https://docs.pola.rs/py-
                        polars/html/reference/api/polars.concat.html> for more information.

Provide input filenames via stdin. Example: find path/to/ -name '*.csv' | python3 -m joinem
-o out.csv
```
