import logging
import typing

import polars as pl


# adapted from https://github.com/mmore500/phyloframe/blob/306926639a012eb65db8ec8c123dda9568f519b7/phyloframe/legacy/_alifestd_pipe_unary_ops_polars.py#L20
def ChainOp(
    *ops: typing.Callable[
        [typing.Union[pl.DataFrame, pl.LazyFrame]],
        typing.Union[pl.DataFrame, pl.LazyFrame],
    ],
    progress_wrap: typing.Callable = lambda x: x,
) -> typing.Callable[
    [typing.Union[pl.DataFrame, pl.LazyFrame]],
    typing.Union[pl.DataFrame, pl.LazyFrame],
]:
    """Create a compound op that applies `ops` in order.

    Returned callable is suitable for `df.pipe(...)`, including as a single
    `--ipipe` or `--opipe` expression.

    Parameters
    ----------
    *ops : callable
        Zero or more callables, each accepting and returning a frame.
    progress_wrap : callable, optional
        Optional wrapper for ops to provide progress feedback (e.g. tqdm).

    Raises
    ------
    ValueError
        If any item of `ops` is not callable.

    Examples
    --------
    >>> df.pipe(ChainOp(SampleOp(10, seed=1), pl.LazyFrame.unique))
    """
    for i, op in enumerate(ops):
        if not callable(op):
            raise ValueError(f"op {i} is not callable, got {op!r}")

    def do_chain(
        df: typing.Union[pl.DataFrame, pl.LazyFrame],
    ) -> typing.Union[pl.DataFrame, pl.LazyFrame]:
        for i, op in enumerate(progress_wrap(ops)):
            logging.info(
                f"- ChainOp: applying op {i + 1} of {len(ops)} {op!r}...",
            )
            df = op(df)
        logging.info("- ChainOp: all ops applied")
        return df

    return do_chain
