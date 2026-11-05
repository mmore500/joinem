import numpy as np
import polars as pl
import pytest

from joinem.exprlib import standard_scaled


@pytest.mark.parametrize("as_str", [True, False])
def test_standard_scaled(as_str: bool):
    values = np.random.normal(5, 3, 100)
    df = pl.DataFrame({"x": values})
    expr = "x" if as_str else pl.col("x")
    res = df.select(standard_scaled(expr))
    assert res.columns == ["x"]
    np.testing.assert_allclose(
        res["x"].to_numpy(), (values - values.mean()) / values.std()
    )


def test_standard_scaled_derived_expr():
    df = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
    res = df.select(standard_scaled(pl.col("x") * 10)).to_series()
    np.testing.assert_allclose(
        res.to_numpy(), [-1.224745, 0.0, 1.224745], rtol=1e-6
    )


def test_standard_scaled_constant():
    df = pl.DataFrame({"x": [4.0, 4.0, 4.0]})
    assert df.select(standard_scaled("x"))["x"].to_list() == [0.0] * 3


def test_standard_scaled_nulls():
    df = pl.DataFrame({"x": [1.0, None, 3.0]})
    assert df.select(standard_scaled("x"))["x"].to_list() == [-1.0, None, 1.0]


def test_standard_scaled_lazy_over():
    df = pl.LazyFrame({"g": [0, 0, 1, 1], "x": [1.0, 3.0, 10.0, 30.0]})
    res = df.with_columns(standard_scaled("x").over("g")).collect()
    assert res["x"].to_list() == [-1.0, 1.0, -1.0, 1.0]
