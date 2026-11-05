import typing

import polars as pl
import pytest

from joinem.exprlib import kdtree_group_id
from joinem.pipelib import ChainOp, SampleOp

_apply_params = pytest.mark.parametrize(
    "apply",
    [
        pytest.param(lambda x: x, id="DataFrame"),
        pytest.param(lambda x: x.lazy(), id="LazyFrame"),
    ],
)


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({"v": range(10)})


@_apply_params
def test_ChainOp_no_ops(df: pl.DataFrame, apply: typing.Callable):
    frame = apply(df)
    assert frame.pipe(ChainOp()) is frame


@_apply_params
def test_ChainOp_multiple_ops(df: pl.DataFrame, apply: typing.Callable):
    op = ChainOp(
        lambda df: df.with_columns(pl.lit(1).alias("col_a")),
        lambda df: df.with_columns((pl.col("col_a") + 1).alias("col_b")),
    )
    result = apply(df).pipe(op)
    assert isinstance(result, type(apply(df)))
    collected = result.lazy().collect()
    assert collected.columns == ["v", "col_a", "col_b"]
    assert (collected["col_b"] == 2).all()


def test_ChainOp_order(df: pl.DataFrame):
    log = []

    def op_a(df):
        log.append("a")
        return df

    def op_b(df):
        log.append("b")
        return df

    op = ChainOp(op_a, op_b)
    assert log == []  # deferred until applied
    df.pipe(op)
    assert log == ["a", "b"]
    df.pipe(op)
    assert log == ["a", "b"] * 2


def test_ChainOp_progress_wrap(df: pl.DataFrame):
    wrapped = []

    def progress_wrap(ops):
        wrapped.append(len(ops))
        return ops

    op = ChainOp(pl.DataFrame.reverse, progress_wrap=progress_wrap)
    assert df.pipe(op)["v"].to_list() == list(range(9, -1, -1))
    assert wrapped == [1]


def test_ChainOp_nested(df: pl.DataFrame):
    reverse_twice = ChainOp(pl.DataFrame.reverse, pl.DataFrame.reverse)
    op = ChainOp(reverse_twice, pl.DataFrame.reverse, reverse_twice)
    assert df.pipe(op)["v"].to_list() == list(range(9, -1, -1))


def test_ChainOp_with_SampleOp():
    df = pl.DataFrame({"x": range(100), "y": range(100, 0, -1)})
    op = ChainOp(
        lambda df: df.with_columns(kdtree_group_id(["x", "y"], n_group=10)),
        SampleOp(1, stratify="kdtree_group_id", seed=1),
        lambda df: df.sort("kdtree_group_id"),
    )
    result = df.lazy().pipe(op).collect()
    assert result["kdtree_group_id"].to_list() == list(range(10))


@pytest.mark.parametrize("bad_op", [None, 1, "op", pl.col("v")])
def test_ChainOp_invalid(bad_op):
    with pytest.raises(ValueError):
        ChainOp(pl.DataFrame.reverse, bad_op)
