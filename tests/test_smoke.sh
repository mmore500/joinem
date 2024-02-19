#!/bin/bash

set -e
set -u

cd "$(dirname "$0")"

mkdir -p out || :

python3 -m joinem --help
python3 -m joinem --version


find assets/*.csv assets/*.pqt assets/*.parquet | python3 -m joinem out/all.csv --how diagonal_relaxed --progress
find assets/*.csv assets/*.pqt assets/*.parquet | python3 -m joinem out/all.fea --how diagonal_relaxed
find assets/*.csv assets/*.pqt assets/*.parquet | python3 -m joinem out/all.feather --how diagonal_relaxed
find assets/*.csv assets/*.pqt assets/*.parquet | python3 -m joinem out/all.json --how diagonal_relaxed
find assets/*.csv assets/*.pqt assets/*.parquet | python3 -m joinem out/all.parquet --how diagonal_relaxed
find assets/*.csv assets/*.pqt assets/*.parquet | python3 -m joinem out/all.pqt --how diagonal_relaxed

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
find assets/x.*parquet assets/y.*csv | python3 -m joinem out/xy.fea --how diagonal
find assets/x.*pqt assets/y.*parquet | python3 -m joinem out/xy.feather --how diagonal
find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy.json --how diagonal
find assets/x.*pqt assets/y.*csv | python3 -m joinem out/xy.parquet --how diagonal
find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy.pqt --how diagonal
