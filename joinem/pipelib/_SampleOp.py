import numbers
import typing

import polars as pl


def SampleOp(
    n: typing.Union[int, pl.Expr, None] = None,
    *,
    fraction: typing.Union[float, pl.Expr, None] = None,
    with_replacement: bool = False,
    shuffle: bool = False,
    seed: typing.Optional[int] = None,
    stratify: typing.Union[
        pl.Expr, str, typing.Iterable[typing.Union[pl.Expr, str]], None
    ] = None,
) -> typing.Callable[[typing.Union[pl.DataFrame, pl.LazyFrame]], pl.LazyFrame]:
    """Create a callable that samples rows from a frame.

    Sampling parameters mirror `polars.DataFrame.sample`. Returned callable
    accepts a `pl.DataFrame` or `pl.LazyFrame` and returns a
    `pl.LazyFrame`, so is suitable for `df.pipe(...)`.

    Parameters
    ----------
    n : int or pl.Expr, optional
        Number of rows to sample (per stratum, if stratified). An
        expression such as `pl.len().clip(upper_bound=100)` samples up to a
        maximum. Mutually exclusive with `fraction`. If neither is given,
        samples one row.
    fraction : float or pl.Expr, optional
        Fraction of rows to sample (per stratum, if stratified).
    with_replacement : bool, default False
        Allow rows to be sampled more than once.
    shuffle : bool, default False
        Shuffle order of sampled rows.
    seed : int, optional
        Seed for deterministic behavior.
    stratify : str, pl.Expr, or iterable thereof, optional
        Column name(s) or expression(s) to stratify by; `n`/`fraction`
        applies separately to each distinct value (or combination of
        values). Strings are interpreted as column names. Stratification
        collects the frame.

    Raises
    ------
    ValueError
        If both `n` and `fraction` are given, if `n` is not an int or
        expression, if `n` or `fraction` is negative, if `fraction` exceeds
        1 without `with_replacement`, or if `stratify` contains an item
        that is not a str or expression.

    Examples
    --------
    >>> df.pipe(SampleOp(2, stratify=["a", pl.col("b") > 0], seed=1))
    """
    if n is not None and fraction is not None:
        raise ValueError("cannot specify both n and fraction")
    if n is not None and not isinstance(n, pl.Expr):
        if isinstance(n, bool) or not isinstance(n, numbers.Integral):
            raise ValueError(
                f"n must be int or pl.Expr, got {n!r}; "
                "use fraction= to sample a fraction of rows",
            )
        if n < 0:
            raise ValueError(f"n must be non-negative, got {n}")
    if fraction is not None and not isinstance(fraction, pl.Expr):
        if isinstance(fraction, bool) or not isinstance(
            fraction, numbers.Real
        ):
            raise ValueError(
                f"fraction must be float or pl.Expr, got {fraction!r}"
            )
        if fraction < 0:
            raise ValueError(f"fraction must be non-negative, got {fraction}")
        if fraction > 1 and not with_replacement:
            raise ValueError(
                f"fraction must be at most 1 without with_replacement, "
                f"got {fraction}",
            )

    if stratify is None:
        stratify = []
    elif isinstance(stratify, (str, pl.Expr)):
        stratify = [stratify]
    stratify = list(stratify)
    for item in stratify:
        if not isinstance(item, (str, pl.Expr)):
            raise ValueError(
                f"stratify items must be str or pl.Expr, {item!r}"
            )

    tmp_name = "__joinem_sampled__"

    def sample_op(
        df: typing.Union[pl.DataFrame, pl.LazyFrame],
    ) -> pl.LazyFrame:
        lf = df.lazy()
        sampled = pl.struct(pl.all()).sample(
            **({"n": n} if fraction is None else {"fraction": fraction}),
            with_replacement=with_replacement,
            shuffle=shuffle,
            seed=seed,
        )
        if stratify:
            # shuffle first so a shared seed picks different rows per group
            lf = lf.collect().sample(fraction=1, shuffle=True, seed=seed)
            lf = lf.lazy()
            sampled = sampled.over(*stratify, mapping_strategy="explode")
        return lf.select(sampled.alias(tmp_name)).unnest(tmp_name)

    return sample_op
