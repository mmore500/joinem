import typing

import polars as pl


def minmax_scaled(expr: typing.Union[pl.Expr, str]) -> pl.Expr:
    """Linearly rescale `expr` onto the range [0, 1].

    Mirrors `sklearn.preprocessing.MinMaxScaler`: maps constant input to
    zero. Output keeps the name of `expr`.
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    lo, hi = expr.min(), expr.max()
    span = hi - lo
    return (expr - lo) / pl.when(span == 0).then(1.0).otherwise(span)
