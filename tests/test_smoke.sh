#!/bin/bash

set -e
set -u

cd "$(dirname "$0")"

mkdir -p out || :

python3 -m joinem --help
python3 -m joinem --version


find assets/*.csv assets/*.csv.gz assets/*.csv.zst assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.csv --how diagonal_relaxed --progress --eager-read --sample 10
find assets/*.csv assets/*.csv.gz assets/*.csv.zst assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.fea --how diagonal_relaxed --drop "a" --tail 10 --shuffle
find assets/*.csv assets/*.csv.gz assets/*.csv.zst assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.fea --how diagonal_relaxed --select "a" --tail 10 --shuffle
find assets/*.csv assets/*.csv.gz assets/*.csv.zst assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.feather --how diagonal_relaxed
find assets/*.csv assets/*.csv.gz assets/*.csv.zst assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.json --how diagonal_relaxed --shrink-dtypes --gather-every 3
find assets/*.csv assets/*.csv.gz assets/*.csv.zst assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.parquet --how diagonal_relaxed --shuffle
find assets/*.csv assets/*.csv.gz assets/*.csv.zst assets/*.pqt assets/*.parquet assets/*.fea assets/*.json* assets/*.feather | python3 -m joinem out/all.pqt --how diagonal_relaxed --write-kwarg 'compression="lz4"' --write-kwarg 'maintain_order=False' --tail 10 --gather-every 1

find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.csv --progress --shrink-dtypes
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.fea --head 1
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.feather
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.json
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.parquet
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.pqt

find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.csv --progress
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.fea
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.feather
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.json --shrink-dtypes
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.parquet
find assets/x*.csv assets/x*.pqt assets/x*.parquet | python3 -m joinem out/x.pqt --sample 4 --seed 2

find assets/x.*pqt assets/y.*csv | python3 -m joinem out/xy.csv --how diagonal --progress
find assets/x.*parquet assets/y.*csv | python3 -m joinem out/xy.fea --how diagonal --string-cache --shrink-dtypes
find assets/x.*pqt assets/y.*parquet | python3 -m joinem out/xy.feather --how diagonal --shrink-dtypes --sample 4
find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy.json --how diagonal --head 100 --tail 2 --sample 1
find assets/x.*pqt assets/y.*csv | python3 -m joinem out/xy.parquet --how diagonal --shrink-dtypes --tail 10
find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy.pqt --how diagonal --head 2 --tail 1 --seed 1

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy2.csv  --how diagonal --filter True --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")' --eager-read

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy3.csv  --how diagonal --filter 'pl.col("filename stem") == ".csv"' --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")' --eager-read --shrink-dtypes

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy3.csv  --how diagonal --filter 'pl.col("filename stem") == ".csv"' --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")' --with-column '*(pl.lit(i).alias(f"{i}") for i in range(10))' --shrink-dtypes

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy4.csv  --how diagonal --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")' --eager-read

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy5.csv  --how diagonal --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")'

find assets/x.*csv assets/y.*csv | python3 -m joinem out/xy6.csv  --how diagonal --with-column 'pl.lit(filepath).str.replace(r".*/(.*)\.csv", r"${1}").alias("filename stem")' --eager-write --shrink-dtypes

find assets/x.*pqt | python3 -m joinem out/x2.csv --with-column 'pl.lit(2).alias("two")' --with-column 'pl.lit(filepath).str.replace(r".*?([^/]*)\.csv", r"${1}").alias("filename stem")' --with-column 'pl.col("a").alias("a2")' --shrink-dtypes

find assets/z.*csv | python3 -m joinem out/z0.pqt --with-column 'pl.col("q").cast(pl.Categorical)' --string-cache --shrink-dtypes --write-kwarg 'compression_level=10' --write-kwarg 'compression="zstd"'

cat assets/x.0.csv | python3 -m joinem "/dev/stdout" --stdin --read-kwarg infer_schema_length=None --output-filetype csv --input-filetype csv > out/stdout

cat assets/x.0.csv.gz | python3 -m joinem "/dev/stdout" --stdin --output-filetype csv --input-filetype csv --shrink-dtypes > out/stdout

cat assets/x.0.csv.gz | python3 -m joinem "/dev/stdout" --stdin --output-filetype csv --input-filetype csv.gz --string-cache > out/stdout

# compressed input and output
find assets/x.*.json assets/x.*.json.gz assets/x.*.json.zst | python3 -m joinem out/xc.json
find assets/x.*.csv.gz | python3 -m joinem out/xc.csv.gz
find assets/x.*.csv.gz | python3 -m joinem out/xc2.csv.gz --eager-read --eager-write --progress
find assets/x.*.csv assets/x.*.csv.gz assets/x.*.csv.zst | python3 -m joinem out/xc.csv.zst
find assets/x.*.csv.zst | python3 -m joinem out/xc2.csv.zst --eager-read --eager-write --progress
cat assets/x.0.csv.zst | python3 -m joinem "/dev/stdout" --stdin --input-filetype csv.zst --output-filetype csv.zst > out/stdout.csv.zst
find assets/x.*.json | python3 -m joinem out/xc.json.gz
find assets/x.*.json | python3 -m joinem out/xc.json.zst --eager-write
find assets/x.*.json | python3 -m joinem out/xc2.json.zst --progress
cat assets/x.0.csv | python3 -m joinem "/dev/stdout" --stdin --input-filetype csv --output-filetype csv.gz > out/stdout.csv.gz
cat assets/x.0.json.zst | python3 -m joinem "/dev/stdout" --stdin --input-filetype json.zst --output-filetype json.gz > out/stdout.json.gz
python3 - <<'PY'
import gzip
import polars as pl

with gzip.open("out/xc.csv.gz") as f:
    assert pl.read_csv(f.read()).equals(pl.read_csv("out/xc2.csv.gz"))
assert pl.read_csv("out/xc.csv.gz").height == 6
assert pl.read_csv("out/stdout.csv.gz").equals(pl.read_csv("assets/x.0.csv"))
for p in "out/xc.json.gz", "out/xc.json.zst", "out/xc2.json.zst":
    assert pl.read_ndjson(p).equals(pl.read_ndjson("out/xc.json.gz")), p
assert pl.read_ndjson("out/xc.json").height == 9
assert pl.read_csv("out/xc.csv.zst").height == 18
assert pl.read_csv("out/xc2.csv.zst").equals(pl.read_csv("out/xc.csv.gz"))
assert pl.read_csv("out/stdout.csv.zst").equals(pl.read_csv("assets/x.0.csv"))
assert pl.read_ndjson("out/stdout.json.gz").equals(
    pl.read_ndjson("assets/x.0.json")
)
PY

# jsonl and ndjson aliases
for ext in jsonl jsonl.gz jsonl.zst ndjson ndjson.gz ndjson.zst; do
  find assets/x.*.json | python3 -m joinem "out/xa.${ext}"
  find assets/x.*.json | python3 -m joinem "out/xa2.${ext}" --eager-write
  echo "out/xa.${ext}" | python3 -m joinem "out/xa_${ext}.json"
  echo "out/xa2.${ext}" | python3 -m joinem "out/xa2_${ext}.json" --eager-read
  cmp "out/xa_${ext}.json" out/xa_jsonl.json
  cmp "out/xa2_${ext}.json" out/xa_jsonl.json
done
cat out/xa.ndjson.zst | python3 -m joinem /dev/stdout --stdin --input-filetype ndjson.zst --output-filetype jsonl.gz > out/stdout.jsonl.gz
python3 -c 'import polars as pl; assert pl.read_ndjson("out/stdout.jsonl.gz").equals(pl.read_ndjson("out/xa_jsonl.json"))'

ls assets/horizontal{1,2}.pqt | python3 -m joinem out/horizontal.csv --how horizontal

ls assets/horizontal{1,2}.pqt | python3 -m joinem out/horizontal.pqt --how horizontal --shrink-dtypes

find assets/x.*csv | python3 -m joinem out/init_eval.csv --init-eval 'pl.Config.set_engine_affinity(engine="streaming")'

find assets/x.*csv assets/y.*csv | python3 -m joinem out/init_eval2.csv --how diagonal --init-eval 'pl.Config.set_engine_affinity(engine="streaming")' --init-eval 'pl.Config.set_tbl_rows(5)' --with-column 'pl.lit(2).alias("two")'

# --sample with pl.len().clip(upper_bound=N) samples up to N rows: fewer rows than requested keeps all rows, otherwise caps
find assets/x.0.csv | python3 -m joinem out/su_all.csv
n_all=$(( $(wc -l < out/su_all.csv) - 1 ))
find assets/x.0.csv | python3 -m joinem out/su_big.csv --sample "pl.len().clip(upper_bound=$(( n_all + 100 )))"
[ "$(( $(wc -l < out/su_big.csv) - 1 ))" -eq "$n_all" ]
find assets/x.0.csv | python3 -m joinem out/su_small.csv --sample "pl.len().clip(upper_bound=1)" --seed 1
[ "$(( $(wc -l < out/su_small.csv) - 1 ))" -eq 1 ]
find assets/x.0.csv | python3 -m joinem out/su_small2.csv --sample "pl.len().clip(upper_bound=1)" --seed 1
cmp out/su_small.csv out/su_small2.csv

# --sample-stratify samples n (or a fraction) per distinct combination
python3 -c 'import polars as pl; pl.DataFrame({"g": [0] * 6 + [1] * 4, "h": [0, 1] * 5, "v": range(10)}).write_csv("out/strat_in.csv")'
find out/strat_in.csv | python3 -m joinem out/strat1.csv --sample 2 --sample-stratify g --seed 1
python3 -c 'import polars as pl; d = pl.read_csv("out/strat1.csv"); assert d["g"].value_counts()["count"].to_list() == [2, 2], d'
find out/strat_in.csv | python3 -m joinem out/strat2.csv --sample 1 --sample-stratify g --sample-stratify h --seed 1
python3 -c 'import polars as pl; d = pl.read_csv("out/strat2.csv"); assert d.height == 4 and d.select(["g", "h"]).n_unique() == 4, d'
find out/strat_in.csv | python3 -m joinem out/strat3.csv --sample 0.5 --sample-stratify g --seed 1
python3 -c 'import polars as pl; d = pl.read_csv("out/strat3.csv"); assert d.filter(pl.col("g") == 0).height == 3 and d.filter(pl.col("g") == 1).height == 2, d'
find out/strat_in.csv | python3 -m joinem out/strat5.csv --sample "pl.len().clip(upper_bound=5)" --sample-stratify g --seed 1
python3 -c 'import polars as pl; d = pl.read_csv("out/strat5.csv"); assert d.filter(pl.col("g") == 0).height == 5 and d.filter(pl.col("g") == 1).height == 4, d'
python3 -c 'import polars as pl; pl.DataFrame({"g": [0, 1] * 100, "v": range(200)}).write_csv("out/strat_big.csv")'
find out/strat_big.csv | python3 -m joinem out/strat4.csv --sample 10 --sample-stratify g --seed 1
python3 -c 'import polars as pl; d = pl.read_csv("out/strat4.csv"); s = [set(d.filter(pl.col("g") == g)["v"]) for g in (0, 1)]; assert len(s[0]) == len(s[1]) == 10 and s[0] != s[1], d'
find out/strat_in.csv | python3 -m joinem out/strat6.csv --with-column 'pl.col("v").ge(3).alias("v3")' --sample 1 --sample-stratify v3 --seed 1
python3 -c 'import polars as pl; d = pl.read_csv("out/strat6.csv"); assert d.height == 2 and d["v3"].n_unique() == 2, d'
find out/strat_in.csv | python3 -m joinem out/frac.csv --sample 0.5 --seed 1
[ "$(( $(wc -l < out/frac.csv) - 1 ))" -eq 5 ]

find assets/x.*csv | python3 -m joinem out/ipipe.csv --ipipe 'lambda df: op(df).with_columns(pl.lit(1).alias("one"))'

find assets/x.*csv | python3 -m joinem out/opipe.csv --opipe 'pl.LazyFrame.describe'

# README --ipipe/--opipe example
find assets/x.*csv | python3 -m joinem out/pipe_readme.pqt \
  --ipipe 'lambda df: df.with_columns(pl.len().alias("n"))' \
  --opipe 'pl.LazyFrame.unique'

# --shrink-dtypes should narrow numeric columns (Int64 a, Float64 b in source)
find assets/x.*csv | python3 -m joinem out/shrink.pqt --shrink-dtypes
python3 -c 'import polars as pl; s = pl.read_parquet("out/shrink.pqt").schema; assert s["a"] != pl.Int64 and s["b"] == pl.Float32, s'
