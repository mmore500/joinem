import numpy as np
import polars as pl
import pytest

from joinem.exprlib import group_id_kdtree


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


@pytest.mark.parametrize(
    "kdtree_kwargs",
    [{"balanced_tree": False}, {"balanced_tree": True}, {"copy_data": True}],
)
def test_kdtree_kwargs(df: pl.DataFrame, kdtree_kwargs: dict):
    res = df.select(
        group_id_kdtree(["x", "y"], n_group=10, kdtree_kwargs=kdtree_kwargs),
    ).to_series()
    assert res.n_unique() == 10


def test_kdtree_kwargs_invalid(df: pl.DataFrame):
    with pytest.raises(Exception):
        df.select(
            group_id_kdtree(["x"], n_group=10, kdtree_kwargs={"nope": 1})
        )


@pytest.mark.parametrize("leaf_size", [1, 16, 1000])
def test_kdtree_kwargs_leaf_size_disallowed(df: pl.DataFrame, leaf_size: int):
    with pytest.raises(Exception):
        df.select(
            group_id_kdtree(
                ["x", "y"], n_group=10, kdtree_kwargs={"leafsize": leaf_size}
            ),
        )
