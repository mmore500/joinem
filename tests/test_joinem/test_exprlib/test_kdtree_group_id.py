import numpy as np
import polars as pl
import pytest

from joinem.exprlib import kdtree_group_id as group_id
from joinem.exprlib import standard_scaled

NAME = "kdtree_group_id"


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "x": np.random.rand(1000),
            "y": np.random.rand(1000) * 100,
            "z": np.random.rand(1000),
            "g": np.random.randint(0, 3, 1000),
        },
    )


@pytest.mark.parametrize("n_group", [1, 2, 7, 64, 100, 500])
@pytest.mark.parametrize("on_dims", [["x"], ["x", "y"], ["x", "y", "z"]])
def test_group_count(df: pl.DataFrame, n_group: int, on_dims: list):
    res = df.select(group_id(on_dims, n_group)).to_series()
    assert res.name == NAME
    assert res.dtype == pl.UInt32
    assert res.null_count() == 0
    assert res.n_unique() == n_group
    assert sorted(res.unique()) == list(range(n_group))


@pytest.mark.parametrize("fraction", [0.001, 0.01, 0.1, 0.5])
def test_group_count_fraction(df: pl.DataFrame, fraction: float):
    res = df.select(group_id(["x", "y"], fraction)).to_series()
    assert res.n_unique() == max(1, int(fraction * len(df)))


@pytest.mark.parametrize("n_group", [1000, 5000, 1.0])
def test_group_count_saturated(df: pl.DataFrame, n_group):
    res = df.select(group_id(["x", "y"], n_group)).to_series()
    assert res.n_unique() == len(df)


def test_group_count_near_saturated(df: pl.DataFrame):
    res = df.select(group_id(["x", "y"], 999)).to_series()
    assert res.n_unique() == 999


def test_duplicate_points():
    df = pl.DataFrame({"x": [1.0] * 10 + [2.0] * 10})
    res = df.with_columns(group_id(["x"], 4))
    assert 2 <= res[NAME].n_unique() <= 4
    assert res.select(pl.col(NAME).n_unique().over("x").max()).item() >= 1
    assert res.select(pl.col("x").n_unique().over(NAME).max()).item() == 1


@pytest.mark.parametrize("n_group", [2, 16, 100])
def test_group_sizes_balanced(df: pl.DataFrame, n_group: int):
    counts = df.select(group_id(["x", "z"], n_group)).to_series()
    sizes = counts.value_counts()["count"]
    assert sizes.max() <= 2 * sizes.min() + 1


def test_spatially_compact():
    # two equal-sized, well-separated clusters, in shuffled order
    cluster = np.random.permutation([0, 1] * 100)
    df = pl.DataFrame(
        {
            "x": np.random.rand(200) + 100 * cluster,
            "y": np.random.rand(200) - 100 * cluster,
            "cluster": cluster,
        },
    )
    res = df.with_columns(group_id(["x", "y"], 2))
    assert res.select(pl.col("cluster", NAME).n_unique()).row(0) == (2, 2)
    assert res.select(pl.struct("cluster", NAME).n_unique()).item() == 2


def test_deterministic(df: pl.DataFrame):
    first = df.select(group_id(["x", "y"], 10))
    second = df.select(group_id(["x", "y"], 10))
    assert first.equals(second)


def test_expr_dims(df: pl.DataFrame):
    res = df.select(
        group_id([standard_scaled("x"), standard_scaled("x") * 2], 10),
    ).to_series()
    assert res.n_unique() == 10


@pytest.mark.parametrize("dim", ["x", pl.col("x")])
def test_scalar_dim(df: pl.DataFrame, dim):
    assert df.select(group_id(dim, 4)).to_series().n_unique() == 4


def test_matches_1d_quantiles():
    df = pl.DataFrame({"x": np.random.permutation(100).astype(float)})
    res = df.with_columns(group_id(["x"], 2))
    lo = res.filter(pl.col("x") < 50)[NAME].unique()
    hi = res.filter(pl.col("x") >= 50)[NAME].unique()
    assert len(lo) == len(hi) == 1 and lo[0] != hi[0]


def test_nulls():
    df = pl.DataFrame(
        {
            "x": [1.0, None, 3.0, float("nan"), 2.0, 10.0],
            "y": [0.0, 0.0, 0.0, 0.0, None, 0.0],
        },
    )
    res = df.select(group_id(["x", "y"], 2)).to_series()
    assert res.is_null().to_list() == [False, True, False, True, True, False]
    assert res.drop_nulls().n_unique() == 2


def test_all_null():
    df = pl.DataFrame({"x": [None, None]}, schema={"x": pl.Float64})
    assert df.select(group_id(["x"], 2)).to_series().null_count() == 2


def test_empty():
    df = pl.DataFrame({"x": []}, schema={"x": pl.Float64})
    res = df.select(group_id(["x"], 2)).to_series()
    assert res.len() == 0 and res.dtype == pl.UInt32


def test_over(df: pl.DataFrame):
    res = df.with_columns(group_id(["x", "y"], 5).over("g"))
    per_g = res.group_by("g").agg(pl.col(NAME).n_unique())[NAME]
    assert per_g.to_list() == [5] * 3


def test_lazy(df: pl.DataFrame):
    expected = df.select(group_id(["x", "y"], 10))
    actual = df.lazy().select(group_id(["x", "y"], 10)).collect()
    assert expected.equals(actual)


@pytest.mark.parametrize(
    "on_dims, n_group",
    [
        ([], 2),
        ([1], 2),
        (["x"], 0),
        (["x"], -1),
        (["x"], 0.0),
        (["x"], 1.5),
        (["x"], -0.5),
        (["x"], True),
        (["x"], "2"),
        (["x"], None),
    ],
)
def test_invalid(on_dims, n_group):
    with pytest.raises(ValueError):
        group_id(on_dims, n_group)


@pytest.mark.parametrize("balanced_tree", [True, False])
def test_kdtree_kwargs(df: pl.DataFrame, balanced_tree: bool):
    res = df.select(
        group_id(
            ["x", "y"], 10, kdtree_kwargs={"balanced_tree": balanced_tree}
        ),
    ).to_series()
    assert res.n_unique() == 10


def test_kdtree_kwargs_leafsize(df: pl.DataFrame):
    # huge leaves leave nothing to split
    res = df.select(
        group_id(["x", "y"], 10, kdtree_kwargs={"leafsize": len(df)}),
    ).to_series()
    assert res.n_unique() == 1


def test_kdtree_kwargs_invalid(df: pl.DataFrame):
    with pytest.raises(Exception):
        df.select(group_id(["x"], 10, kdtree_kwargs={"nope": 1}))
