#!/bin/bash

set -e
set -u

cd "$(dirname "$0")"

mkdir -p out || :

python3 -m joinem --help
python3 -m joinem --version


find assets/*.csv assets/*.csv.gz assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.csv --how diagonal_relaxed --progress --eager-read
find assets/*.csv assets/*.csv.gz assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.fea --how diagonal_relaxed
find assets/*.csv assets/*.csv.gz assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.feather --how diagonal_relaxed
find assets/*.csv assets/*.csv.gz assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.json --how diagonal_relaxed
find assets/*.csv assets/*.csv.gz assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.parquet --how diagonal_relaxed
find assets/*.csv assets/*.csv.gz assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.pqt --how diagonal_relaxed --write-kwarg 'compression="lz4"' --write-kwarg 'maintain_order=False'

find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.csv --progress
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.fea
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.feather
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.json
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.parquet
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.pqt

find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.csv --progress
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.fea
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.feather
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.json
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.parquet
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.pqt

find assets/x.*pqt assets/y.*csv | python3 -m joinem out/xy.csv --how diagonal --progress
find assets/x.*parquet assets/y.*csv | python3 -m joinem out/xy.fea --how diagonal --string-cache
find assets/x.*pqt assets/y.*parquet | python3 -m joinem out/xy.feather --how diagonal
find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy.json --how diagonal
find assets/x.*pqt assets/y.*csv | python3 -m joinem out/xy.parquet --how diagonal
find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy.pqt --how diagonal

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy2.csv  --how diagonal --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")' --eager-read

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy3.csv  --how diagonal --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")' --eager-read

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy4.csv  --how diagonal --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")' --eager-read

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy5.csv  --how diagonal --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")'

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy6.csv  --how diagonal --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")' --eager-write

find assets/x.*pqt | python3 -m joinem out/x2.csv --with-column 'pl.lit(2).alias("two")' --with-column 'pl.lit(filepath).str.replace(r".*?([^/]*)\.csv", r"${1}").alias("filename stem")' --with-column 'pl.col("a").alias("a2")'

cat assets/x.0.csv | python3 -m joinem "/dev/stdout" --stdin --read-kwarg infer_schema_length=None --output-filetype csv --input-filetype csv > out/stdout

cat assets/x.0.csv.gz | python3 -m joinem "/dev/stdout" --stdin --output-filetype csv --input-filetype csv > out/stdout

cat assets/x.0.csv.gz | python3 -m joinem "/dev/stdout" --stdin --output-filetype csv --input-filetype csv.gz --string-cache > out/stdout
