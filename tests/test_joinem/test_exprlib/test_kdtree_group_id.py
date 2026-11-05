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


@pytest.mark.parametrize("n_group", [1, 2, 7, 64, 100, 500, 999, 1000])
@pytest.mark.parametrize("on_dims", [["x"], ["x", "y"], ["x", "y", "z"]])
def test_group_count(df: pl.DataFrame, n_group: int, on_dims: list):
    res = df.select(group_id(on_dims, n_group=n_group)).to_series()
    assert res.name == NAME
    assert res.dtype == pl.UInt64
    assert res.null_count() == 0
    assert sorted(res.unique()) == list(range(n_group))


@pytest.mark.parametrize("fraction", [0.001, 0.01, 0.1, 0.5])
def test_group_count_fraction(df: pl.DataFrame, fraction: float):
    res = df.select(group_id(["x", "y"], n_group=fraction)).to_series()
    assert res.n_unique() == max(1, int(fraction * len(df)))


@pytest.mark.parametrize("n_group", [5000, 1.0])
def test_group_count_clipped(df: pl.DataFrame, n_group):
    res = df.select(group_id(["x", "y"], n_group=n_group)).to_series()
    assert res.n_unique() == len(df)


@pytest.mark.parametrize("n_group", [2, 7, 16, 100])
def test_group_sizes_balanced(df: pl.DataFrame, n_group: int):
    res = df.select(group_id(["x", "z"], n_group=n_group)).to_series()
    sizes = res.value_counts()["count"]
    assert sizes.max() - sizes.min() <= 1


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
    res = df.with_columns(group_id(["x", "y"], n_group=2))
    assert res.select(pl.col("cluster", NAME).n_unique()).row(0) == (2, 2)
    assert res.select(pl.struct("cluster", NAME).n_unique()).item() == 2


def test_spatially_compact_1d():
    df = pl.DataFrame({"x": np.random.permutation(100)})
    res = df.with_columns(group_id(["x"], n_group=10))
    # each group is a contiguous run of x values
    assert res.sort("x")[NAME].rle_id().n_unique() == 10


def test_deterministic(df: pl.DataFrame):
    first = df.select(group_id(["x", "y"], n_group=10))
    second = df.select(group_id(["x", "y"], n_group=10))
    assert first.equals(second)


def test_int_dims():
    df = pl.DataFrame({"x": range(100), "y": [v % 7 for v in range(100)]})
    res = df.select(group_id(["x", "y"], n_group=10)).to_series()
    assert res.n_unique() == 10


def test_expr_dims(df: pl.DataFrame):
    res = df.select(
        group_id([standard_scaled("x"), standard_scaled("x") * 2], n_group=10),
    ).to_series()
    assert res.n_unique() == 10


@pytest.mark.parametrize("dim", ["x", pl.col("x")])
def test_scalar_dim(df: pl.DataFrame, dim):
    assert df.select(group_id(dim, n_group=4)).to_series().n_unique() == 4


def test_duplicate_points():
    df = pl.DataFrame({"x": [1.0] * 10 + [2.0] * 10})
    res = df.with_columns(group_id(["x"], n_group=4))
    assert res[NAME].n_unique() == 4
    assert res.select(pl.col("x").n_unique().over(NAME).max()).item() == 1


def test_nulls():
    df = pl.DataFrame(
        {
            "x": [1.0, None, 3.0, float("nan"), 2.0, 10.0],
            "y": [0.0, 0.0, 0.0, 0.0, None, 0.0],
        },
    )
    res = df.select(group_id(["x", "y"], n_group=2)).to_series()
    assert res.is_null().to_list() == [False, True, False, True, True, False]
    assert res.drop_nulls().n_unique() == 2


def test_int_nulls():
    df = pl.DataFrame({"x": [1, None, 3, 2]})
    res = df.select(group_id(["x"], n_group=3)).to_series()
    assert res.to_list()[1] is None
    assert res.drop_nulls().n_unique() == 3


def test_all_null():
    df = pl.DataFrame({"x": [None, None]}, schema={"x": pl.Float64})
    res = df.select(group_id(["x"], n_group=2)).to_series()
    assert res.null_count() == 2


def test_empty():
    df = pl.DataFrame({"x": []}, schema={"x": pl.Float64})
    res = df.select(group_id(["x"], n_group=2)).to_series()
    assert res.len() == 0 and res.dtype == pl.UInt64


def test_over(df: pl.DataFrame):
    res = df.with_columns(group_id(["x", "y"], n_group=5).over("g"))
    per_g = res.group_by("g").agg(pl.col(NAME).n_unique())[NAME]
    assert per_g.to_list() == [5] * 3


def test_lazy(df: pl.DataFrame):
    expected = df.select(group_id(["x", "y"], n_group=10))
    actual = df.lazy().select(group_id(["x", "y"], n_group=10)).collect()
    assert expected.equals(actual)


def test_n_group_keyword_only():
    with pytest.raises(TypeError):
        group_id(["x"], 2)


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
        group_id(on_dims, n_group=n_group)


@pytest.mark.parametrize(
    "kdtree_kwargs",
    [{"balanced_tree": False}, {"balanced_tree": True}, {"copy_data": True}],
)
def test_kdtree_kwargs(df: pl.DataFrame, kdtree_kwargs: dict):
    res = df.select(
        group_id(["x", "y"], n_group=10, kdtree_kwargs=kdtree_kwargs),
    ).to_series()
    assert res.n_unique() == 10


def test_kdtree_kwargs_invalid(df: pl.DataFrame):
    with pytest.raises(Exception):
        df.select(group_id(["x"], n_group=10, kdtree_kwargs={"nope": 1}))


def test_compact_groups():
    # groups are much tighter than random groups of the same size
    df = pl.DataFrame({"x": np.random.rand(1000), "y": np.random.rand(1000)})
    area = (
        df.with_columns(group_id(["x", "y"], n_group=10))
        .group_by(NAME)
        .agg(
            (pl.col("x").max() - pl.col("x").min())
            * (pl.col("y").max() - pl.col("y").min()),
        )["x"]
    )
    # random groups of 100 uniform points have bounding box area ~0.95
    assert area.max() < 0.5
    assert area.mean() < 0.3


@pytest.mark.parametrize("leaf_size", [1, 16, 1000])
def test_kdtree_kwargs_leaf_size_disallowed(df: pl.DataFrame, leaf_size: int):
    with pytest.raises(Exception):
        df.select(
            group_id(
                ["x", "y"], n_group=10, kdtree_kwargs={"leafsize": leaf_size}
            ),
        )
