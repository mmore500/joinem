import typing

import polars as pl


def scaled_standard(expr: typing.Union[pl.Expr, str]) -> pl.Expr:
    """Standardize `expr` to zero mean and unit variance.

    Mirrors `sklearn.preprocessing.StandardScaler`: uses population
    standard deviation (ddof=0), and maps zero-variance input to zero.
    Output keeps the name of `expr`.
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    std = expr.std(ddof=0)
    return (expr - expr.mean()) / pl.when(std == 0).then(1.0).otherwise(std)
