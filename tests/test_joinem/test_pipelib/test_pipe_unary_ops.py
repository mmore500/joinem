import typing

import polars as pl
import pytest

from joinem.pipelib import pipe_unary_ops

_apply_params = pytest.mark.parametrize(
    "apply",
    [
        pytest.param(lambda x: x, id="DataFrame"),
        pytest.param(lambda x: x.lazy(), id="LazyFrame"),
    ],
)


def _collect(frame: typing.Union[pl.DataFrame, pl.LazyFrame]) -> pl.DataFrame:
    return frame.lazy().collect()


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({"v": range(10)})


@_apply_params
def test_pipe_unary_ops_no_ops(df: pl.DataFrame, apply: typing.Callable):
    frame = apply(df)
    result = pipe_unary_ops(frame)
    assert result is frame


@_apply_params
def test_pipe_unary_ops_single_op(df: pl.DataFrame, apply: typing.Callable):
    result = pipe_unary_ops(
        apply(df),
        lambda df: df.with_columns(pl.lit(True).alias("test_col")),
    )
    assert isinstance(result, type(apply(df)))
    collected = _collect(result)
    assert "test_col" in collected.columns
    assert collected["test_col"].all()


@_apply_params
def test_pipe_unary_ops_multiple_ops(df: pl.DataFrame, apply: typing.Callable):
    result = pipe_unary_ops(
        apply(df),
        lambda df: df.with_columns(pl.lit(1).alias("col_a")),
        lambda df: df.with_columns((pl.col("col_a") + 1).alias("col_b")),
    )
    collected = _collect(result)
    assert "col_a" in collected.columns
    assert "col_b" in collected.columns
    assert (collected["col_b"] == 2).all()


def test_pipe_unary_ops_order(df: pl.DataFrame):
    log = []

    def op_a(df):
        log.append("a")
        return df

    def op_b(df):
        log.append("b")
        return df

    pipe_unary_ops(df, op_a, op_b)
    assert log == ["a", "b"]


def test_pipe_unary_ops_progress_wrap(df: pl.DataFrame):
    wrapped = []

    def progress_wrap(ops):
        wrapped.append(len(ops))
        return ops

    result = pipe_unary_ops(
        df, pl.DataFrame.reverse, progress_wrap=progress_wrap
    )
    assert wrapped == [1]
    assert result["v"].to_list() == list(range(9, -1, -1))


@pytest.mark.parametrize("bad_op", [None, 1, "op", pl.col("v")])
def test_pipe_unary_ops_invalid(df: pl.DataFrame, bad_op):
    with pytest.raises(ValueError):
        pipe_unary_ops(df, pl.DataFrame.reverse, bad_op)
