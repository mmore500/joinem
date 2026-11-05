import polars as pl
import pytest

from joinem.exprlib import kdtree_group_id
from joinem.pipelib import SampleOp


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame(
        {"g": [0] * 6 + [1] * 4, "h": [0, 1] * 5, "v": range(10)},
    )


@pytest.mark.parametrize("lazy", [True, False])
@pytest.mark.parametrize("stratify", [None, "g"])
def test_return_type(df: pl.DataFrame, lazy: bool, stratify):
    frame = df.lazy() if lazy else df
    res = frame.pipe(SampleOp(1, seed=1, stratify=stratify))
    assert isinstance(res, pl.LazyFrame)
    assert res.collect().columns == df.columns


def test_default_samples_one(df: pl.DataFrame):
    assert df.pipe(SampleOp()).collect().height == 1


@pytest.mark.parametrize("n", [0, 1, 5, 10])
def test_n(df: pl.DataFrame, n: int):
    res = df.pipe(SampleOp(n, seed=1)).collect()
    assert res.height == n
    assert res["v"].is_unique().all()
    assert res.join(df, on=df.columns, how="anti").is_empty()


def test_n_too_large(df: pl.DataFrame):
    with pytest.raises(Exception):
        df.pipe(SampleOp(11)).collect()


def test_n_with_replacement(df: pl.DataFrame):
    res = df.pipe(SampleOp(100, with_replacement=True, seed=1)).collect()
    assert res.height == 100
    assert not res["v"].is_unique().all()


def test_n_expr(df: pl.DataFrame):
    assert (
        df.pipe(SampleOp(pl.len().clip(upper_bound=3))).collect().height == 3
    )
    assert (
        df.pipe(SampleOp(pl.len().clip(upper_bound=99))).collect().height == 10
    )


@pytest.mark.parametrize("fraction, expected", [(0.0, 0), (0.5, 5), (1.0, 10)])
def test_fraction(df: pl.DataFrame, fraction: float, expected: int):
    assert (
        df.pipe(SampleOp(fraction=fraction, seed=1)).collect().height
        == expected
    )


def test_seed_deterministic(df: pl.DataFrame):
    first = df.pipe(SampleOp(5, seed=1, shuffle=True)).collect()
    second = df.pipe(SampleOp(5, seed=1, shuffle=True)).collect()
    assert first.equals(second)


def test_seed_varies():
    df = pl.DataFrame({"v": range(1000)})
    first = df.pipe(SampleOp(10, seed=1)).collect()
    second = df.pipe(SampleOp(10, seed=2)).collect()
    assert not first.equals(second)


def test_shuffle_preserves_rows(df: pl.DataFrame):
    res = df.pipe(SampleOp(10, shuffle=True, seed=1)).collect()
    assert res.sort("v").equals(df)


@pytest.mark.parametrize("stratify", ["g", pl.col("g"), ["g"], ("g",)])
def test_stratify_single(df: pl.DataFrame, stratify):
    res = df.pipe(SampleOp(2, stratify=stratify, seed=1)).collect()
    assert res["g"].value_counts(sort=True)["count"].to_list() == [2, 2]


@pytest.mark.parametrize("stratify", [["g", "h"], ["g", pl.col("h")]])
def test_stratify_multiple(df: pl.DataFrame, stratify):
    res = df.pipe(SampleOp(1, stratify=stratify, seed=1)).collect()
    assert res.height == 4
    assert res.select("g", "h").n_unique() == 4


def test_stratify_derived_expr(df: pl.DataFrame):
    res = df.pipe(SampleOp(1, stratify=pl.col("v") >= 3, seed=1)).collect()
    assert res.columns == df.columns
    assert res.height == 2
    assert (res["v"] >= 3).n_unique() == 2


def test_stratify_fraction(df: pl.DataFrame):
    res = df.pipe(SampleOp(fraction=0.5, stratify="g", seed=1)).collect()
    assert res.filter(pl.col("g") == 0).height == 3
    assert res.filter(pl.col("g") == 1).height == 2


def test_stratify_n_expr(df: pl.DataFrame):
    res = df.pipe(
        SampleOp(pl.len().clip(upper_bound=5), stratify="g", seed=1),
    ).collect()
    assert res.filter(pl.col("g") == 0).height == 5
    assert res.filter(pl.col("g") == 1).height == 4


def test_stratify_seed_varies_by_group():
    df = pl.DataFrame({"g": [0, 1] * 100, "v": range(200)})
    res = df.pipe(SampleOp(10, stratify="g", seed=1)).collect()
    s = [set(res.filter(pl.col("g") == g)["v"] // 2) for g in (0, 1)]
    assert len(s[0]) == len(s[1]) == 10 and s[0] != s[1]


def test_stratify_empty_is_unstratified(df: pl.DataFrame):
    assert df.pipe(SampleOp(3, stratify=[], seed=1)).collect().height == 3


def test_stratify_lazy(df: pl.DataFrame):
    res = df.lazy().pipe(SampleOp(2, stratify="g", seed=1))
    assert isinstance(res, pl.LazyFrame)
    assert res.collect().height == 4


def test_stratify_tree_group_id():
    df = pl.DataFrame({"x": range(100), "y": range(100, 0, -1)})
    group_id = kdtree_group_id(["x", "y"], n_group=10)
    res = df.pipe(SampleOp(1, stratify=group_id, seed=1)).collect()
    assert res.columns == df.columns
    assert res.height == 10
    # exactly one sampled row per group
    groups = df.with_columns(group_id).join(res, on=["x", "y"])
    assert groups["kdtree_group_id"].sort().to_list() == list(range(10))


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(n=1, fraction=0.5),
        dict(n=0.5),
        dict(n=True),
        dict(n="1"),
        dict(n=-1),
        dict(fraction=-0.1),
        dict(fraction=1.5),
        dict(fraction="0.5"),
        dict(n=1, stratify=[1]),
        dict(n=1, stratify=["g", None]),
    ],
)
def test_invalid(kwargs: dict):
    with pytest.raises(ValueError):
        SampleOp(**kwargs)


def test_fraction_with_replacement_over_one(df: pl.DataFrame):
    res = df.pipe(
        SampleOp(fraction=2.0, with_replacement=True, seed=1)
    ).collect()
    assert res.height == 20
