import typing

import polars as pl

from ._pipe_unary_ops import _validate_unary_ops, pipe_unary_ops

_Frame = typing.TypeVar("_Frame", pl.DataFrame, pl.LazyFrame)


def PipeUnaryOpsOp(
    *unary_ops: typing.Callable[[_Frame], _Frame],
    progress_wrap: typing.Callable = lambda x: x,
) -> typing.Callable[[_Frame], _Frame]:
    """Create a compound op that applies `unary_ops` in order.

    Returned callable accepts a frame and returns the result of
    `pipe_unary_ops(df, *unary_ops, progress_wrap=progress_wrap)`, so is
    suitable for `df.pipe(...)`, including as a single `--ipipe` or
    `--opipe` expression.

    Parameters
    ----------
    *unary_ops : callable
        Zero or more callables, each accepting and returning a frame.
    progress_wrap : callable, optional
        Optional wrapper for unary_ops to provide progress feedback (e.g.
        tqdm).

    Raises
    ------
    ValueError
        If any item of `unary_ops` is not callable.

    Examples
    --------
    >>> df.pipe(PipeUnaryOpsOp(SampleOp(10, seed=1), pl.LazyFrame.unique))
    """
    _validate_unary_ops(unary_ops)

    def pipe_unary_ops_op(df: _Frame) -> _Frame:
        return pipe_unary_ops(df, *unary_ops, progress_wrap=progress_wrap)

    return pipe_unary_ops_op
