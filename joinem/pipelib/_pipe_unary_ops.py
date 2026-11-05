import logging
import typing

import polars as pl

_Frame = typing.TypeVar("_Frame", pl.DataFrame, pl.LazyFrame)


def pipe_unary_ops(
    df: _Frame,
    *unary_ops: typing.Callable[[_Frame], _Frame],
    progress_wrap: typing.Callable = lambda x: x,
) -> _Frame:
    """Pipe a DataFrame or LazyFrame through a sequence of unary operations.

    Each operation in `unary_ops` is applied in order to the frame.

    Parameters
    ----------
    df : polars.DataFrame or polars.LazyFrame
        The frame to transform.
    *unary_ops : callable
        Zero or more callables, each accepting and returning a frame.
    progress_wrap : callable, optional
        Optional wrapper for unary_ops to provide progress feedback (e.g.
        tqdm).

    Returns
    -------
    polars.DataFrame or polars.LazyFrame
        The result of piping `df` through each operation in order.

    Raises
    ------
    ValueError
        If any item of `unary_ops` is not callable.

    See Also
    --------
    PipeUnaryOpsOp :
        Factory that bundles `unary_ops` into a single op, for `df.pipe`.
    """
    _validate_unary_ops(unary_ops)
    for i, op in enumerate(progress_wrap(unary_ops)):
        logging.info(
            "- pipe_unary_ops: "
            f"applying op {i + 1} of {len(unary_ops)} {op!r}...",
        )
        df = op(df)
    logging.info("- pipe_unary_ops: all ops applied")
    return df


def _validate_unary_ops(unary_ops: typing.Sequence) -> None:
    for i, op in enumerate(unary_ops):
        if not callable(op):
            raise ValueError(f"unary op {i} is not callable, got {op!r}")
