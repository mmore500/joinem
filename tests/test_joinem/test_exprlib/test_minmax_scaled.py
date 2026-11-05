import numpy as np
import polars as pl
import pytest

from joinem.exprlib import minmax_scaled


@pytest.mark.parametrize("as_str", [True, False])
def test_minmax_scaled(as_str: bool):
    values = np.random.normal(5, 3, 100)
    df = pl.DataFrame({"x": values})
    expr = "x" if as_str else pl.col("x")
    res = df.select(minmax_scaled(expr))
    assert res.columns == ["x"]
    np.testing.assert_allclose(
        res["x"].to_numpy(),
        (values - values.min()) / (values.max() - values.min()),
    )
    assert res["x"].min() == pytest.approx(0.0)
    assert res["x"].max() == pytest.approx(1.0)


def test_minmax_scaled_int():
    df = pl.DataFrame({"x": [2, 4, 6]})
    assert df.select(minmax_scaled("x"))["x"].to_list() == [0.0, 0.5, 1.0]


def test_minmax_scaled_constant():
    df = pl.DataFrame({"x": [4.0, 4.0, 4.0]})
    assert df.select(minmax_scaled("x"))["x"].to_list() == [0.0] * 3


def test_minmax_scaled_nulls():
    df = pl.DataFrame({"x": [1.0, None, 3.0, 2.0]})
    res = df.select(minmax_scaled("x"))["x"].to_list()
    assert res == [0.0, None, 1.0, 0.5]


def test_minmax_scaled_lazy_over():
    df = pl.LazyFrame({"g": [0, 0, 1, 1], "x": [1.0, 3.0, 10.0, 30.0]})
    res = df.with_columns(minmax_scaled("x").over("g")).collect()
    assert res["x"].to_list() == [0.0, 1.0, 0.0, 1.0]
