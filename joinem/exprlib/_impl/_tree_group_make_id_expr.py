import typing

import numpy as np
import polars as pl

from ._tree_group_resolve_n_group import tree_group_resolve_n_group
from ._tree_group_validate_n_group import tree_group_validate_n_group


def tree_group_make_id_expr(
    on_dims: typing.Sequence[typing.Union[pl.Expr, str]],
    *,
    n_group: int,
    label_rows: typing.Callable[[np.ndarray, int], np.ndarray],
    name: str,
) -> pl.Expr:
    """Shared implementation for spatial-tree group id expressions.

    `label_rows(X, n_group)` must return a group id in [0, n_group) for
    each row of `X`, which contains no null or NaN coordinates.
    """
    if isinstance(on_dims, (str, pl.Expr)):
        on_dims = [on_dims]
    on_dims = list(on_dims)
    if not on_dims:
        raise ValueError("on_dims must contain at least one dimension")
    if not all(isinstance(dim, (str, pl.Expr)) for dim in on_dims):
        raise ValueError(f"on_dims must be str or pl.Expr, got {on_dims!r}")
    tree_group_validate_n_group(n_group)

    dims = [
        (pl.col(dim) if isinstance(dim, str) else dim).alias(
            f"_{i}"  # avoid struct field name collisions
        )
        for i, dim in enumerate(on_dims)
    ]

    def apply(series: pl.Series) -> pl.Series:
        coords = series.struct.unnest()
        invalid = coords.select(
            pl.any_horizontal(pl.all().fill_nan(None).is_null()),
        ).to_series()
        X = coords.filter(~invalid).to_numpy()
        result = np.zeros(len(coords), dtype=np.uint64)
        if len(X):
            result[~invalid.to_numpy()] = label_rows(
                X, tree_group_resolve_n_group(n_group, len(X))
            )
        return pl.Series(name, result, dtype=pl.UInt64).set(invalid, None)

    return (
        pl.struct(dims).map_batches(apply, return_dtype=pl.UInt64).alias(name)
    )
