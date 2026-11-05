import numpy as np
import polars as pl
import pytest

from joinem.exprlib import group_id_balltree


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
    "balltree_kwargs",
    [{"metric": "chebyshev"}, {"metric": "manhattan"}],
)
def test_balltree_kwargs(df: pl.DataFrame, balltree_kwargs: dict):
    res = df.select(
        group_id_balltree(
            ["x", "y"], n_group=10, balltree_kwargs=balltree_kwargs
        ),
    ).to_series()
    assert res.n_unique() == 10


def test_balltree_kwargs_invalid(df: pl.DataFrame):
    with pytest.raises(Exception):
        df.select(
            group_id_balltree(
                ["x"], n_group=10, balltree_kwargs={"metric": "nope"}
            ),
        )


@pytest.mark.parametrize("leaf_size", [1, 16, 1000])
def test_balltree_kwargs_leaf_size_disallowed(
    df: pl.DataFrame, leaf_size: int
):
    with pytest.raises(Exception):
        df.select(
            group_id_balltree(
                ["x", "y"],
                n_group=10,
                balltree_kwargs={"leaf_size": leaf_size},
            ),
        )
