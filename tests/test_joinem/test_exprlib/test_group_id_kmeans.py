import warnings

import numpy as np
import polars as pl
import pytest

from joinem.exprlib import group_id_kdtree, group_id_kmeans


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "x": np.random.rand(1000),
            "y": np.random.rand(1000) * 100,
            "g": np.random.randint(0, 3, 1000),
        },
    )


@pytest.mark.parametrize("n_group", [1, 2, 10, 50])
def test_group_count(df: pl.DataFrame, n_group: int):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        res = df.select(
            group_id_kmeans(["x", "y"], n_group=n_group, seed=1),
        ).to_series()
    assert res.name == "group_id_kmeans"
    assert res.dtype == pl.UInt64
    assert res.null_count() == 0
    assert sorted(res.unique()) == list(range(n_group))


def test_seed_deterministic(df: pl.DataFrame):
    first = df.select(group_id_kmeans(["x", "y"], n_group=10, seed=1))
    second = df.select(group_id_kmeans(["x", "y"], n_group=10, seed=1))
    assert first.equals(second)


def test_seed_varies(df: pl.DataFrame):
    first = df.select(group_id_kmeans(["x", "y"], n_group=10, seed=1))
    second = df.select(group_id_kmeans(["x", "y"], n_group=10, seed=2))
    assert not first.equals(second)


def test_spatially_compact():
    # two well-separated clusters, in shuffled order
    cluster = np.random.permutation([0, 1] * 100)
    df = pl.DataFrame(
        {
            "x": np.random.rand(200) + 100 * cluster,
            "y": np.random.rand(200) - 100 * cluster,
            "cluster": cluster,
        },
    )
    res = df.with_columns(group_id_kmeans(["x", "y"], n_group=2, seed=1))
    assert (
        res.select(pl.struct("cluster", "group_id_kmeans").n_unique()).item()
        == 2
    )


def test_covers_space_not_rows():
    # dense blob holds 90% of rows; k-means gives sparse surroundings most
    # groups, whereas equal-count k-d tree groups mostly land in the blob
    df = pl.DataFrame(
        np.r_[
            np.random.normal(0, 0.05, (9000, 2)),
            np.random.uniform(-3, 3, (1000, 2)),
        ],
        schema=["x", "y"],
    ).with_columns(dense=pl.int_range(pl.len()) < 9000)

    def n_dense_groups(group_id: pl.Expr) -> int:
        return (
            df.with_columns(group_id.alias("gid"))
            .group_by("gid")
            .agg(pl.col("dense").mean())
            .filter(pl.col("dense") > 0.5)
            .height
        )

    assert n_dense_groups(group_id_kmeans(["x", "y"], n_group=20, seed=1)) < 10
    assert n_dense_groups(group_id_kdtree(["x", "y"], n_group=20)) > 15


def test_int_dims():
    df = pl.DataFrame({"x": range(100), "y": [v % 7 for v in range(100)]})
    res = df.select(group_id_kmeans(["x", "y"], n_group=5, seed=1))
    assert res.to_series().n_unique() == 5


def test_empty_clusters_warn_and_relabel():
    df = pl.DataFrame({"x": [1.0] * 10 + [2.0] * 10})
    with pytest.warns(UserWarning, match="found only 2 of n_group=4"):
        res = df.select(group_id_kmeans(["x"], n_group=4, seed=1)).to_series()
    assert sorted(res.unique()) == [0, 1]  # ids stay contiguous
    assert (
        res.to_frame()
        .with_columns(df["x"])
        .select(pl.col("x").n_unique().over("group_id_kmeans").max())
        .item()
        == 1
    )


def test_clipped_warns():
    df = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
    with pytest.warns(UserWarning, match="clipping to 3 groups"):
        res = df.select(group_id_kmeans(["x"], n_group=5, seed=1)).to_series()
    assert res.n_unique() == 3


def test_nulls():
    df = pl.DataFrame({"x": [1.0, None, 3.0, float("nan"), 5.0]})
    res = df.select(group_id_kmeans(["x"], n_group=2, seed=1)).to_series()
    assert res.is_null().to_list() == [False, True, False, True, False]
    assert res.drop_nulls().n_unique() == 2


def test_empty():
    df = pl.DataFrame({"x": []}, schema={"x": pl.Float64})
    res = df.select(group_id_kmeans(["x"], n_group=2, seed=1)).to_series()
    assert res.len() == 0 and res.dtype == pl.UInt64


def test_over(df: pl.DataFrame):
    res = df.with_columns(
        group_id_kmeans(["x", "y"], n_group=5, seed=1).over("g"),
    )
    per_g = res.group_by("g").agg(pl.col("group_id_kmeans").n_unique())
    assert per_g["group_id_kmeans"].to_list() == [5] * 3


def test_lazy(df: pl.DataFrame):
    expected = df.select(group_id_kmeans(["x", "y"], n_group=10, seed=1))
    actual = (
        df.lazy()
        .select(group_id_kmeans(["x", "y"], n_group=10, seed=1))
        .collect()
    )
    assert expected.equals(actual)


def test_kmeans_kwargs(df: pl.DataFrame):
    res = df.select(
        group_id_kmeans(
            ["x", "y"],
            n_group=10,
            seed=1,
            kmeans_kwargs={"minit": "points", "iter": 3},
        ),
    ).to_series()
    assert res.n_unique() == 10


def test_kmeans_kwargs_invalid(df: pl.DataFrame):
    with pytest.raises(Exception):
        df.select(
            group_id_kmeans(["x"], n_group=10, kmeans_kwargs={"nope": 1}),
        )


@pytest.mark.parametrize(
    "on_dims, n_group",
    [([], 2), ([1], 2), (["x"], 0), (["x"], 0.5), (["x"], True)],
)
def test_invalid(on_dims, n_group):
    with pytest.raises(ValueError):
        group_id_kmeans(on_dims, n_group=n_group)


def test_n_group_keyword_only():
    with pytest.raises(TypeError):
        group_id_kmeans(["x"], 2)
