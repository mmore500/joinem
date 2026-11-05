import functools
import types
import typing

import numpy as np
import polars as pl
from sklearn.neighbors import BallTree

from ._impl import tree_group_label_tree_order, tree_group_make_id_expr


def _label_rows(
    X: np.ndarray, n_group: int, balltree_kwargs: typing.Mapping
) -> np.ndarray:
    tree = BallTree(X, leaf_size=1, **balltree_kwargs)  # leaves are unordered
    __, tree_order, __, __ = tree.get_arrays()
    return tree_group_label_tree_order(tree_order, n_group)


def balltree_group_id(
    on_dims: typing.Sequence[typing.Union[pl.Expr, str]],
    *,
    n_group: typing.Union[int, float],
    balltree_kwargs: typing.Mapping[str, typing.Any] = types.MappingProxyType(
        {}
    ),
) -> pl.Expr:
    """Get a distinct integer ID for each spatially compact group of rows,
    partitioned via a ball tree.

    The ID starts at 0 and increases by one for each group, up to
    `n_group - 1`. Rows with any null or NaN coordinate get a null ID.

    Parameters
    ----------
    on_dims : sequence of str or pl.Expr
        Coordinates for each row; distance is Euclidean by default.
        Consider scaling with `standard_scaled` or `minmax_scaled` first.
    n_group : int or float
        Number of groups (int), or fraction of non-null rows (float in
        (0, 1]). Clipped to the number of non-null rows.
    balltree_kwargs : mapping, optional
        Additional keyword arguments for `sklearn.neighbors.BallTree`, such
        as `metric`. Passing `leaf_size` raises TypeError, as `leaf_size=1`
        is required for spatially compact groups.

    Returns
    -------
    pl.Expr
        Expression of data type `UInt64`, named "balltree_group_id".

    Raises
    ------
    ValueError
        If `on_dims` is empty, or `n_group` is non-positive or a float
        outside (0, 1].

    See Also
    --------
    kdtree_group_id :
        k-d tree counterpart, with box-shaped rather than ball-shaped
        groups.

    Notes
    -----
    Groups are formed by cutting the tree's traversal order of rows into
    `n_group` contiguous chunks, so group sizes differ by at most one and
    each group occupies one region or a few neighboring regions of the
    tree. Group IDs are deterministic; no seed is needed.

    Groups are computed over all rows in the evaluation context; within
    `.over(...)`, groups are computed separately per window.

    Examples
    --------
    Thin to two spatially spread rows from each of 100 groups:

    >>> SampleOp(2, stratify=balltree_group_id(["x", "y"], n_group=100))
    """
    return tree_group_make_id_expr(
        on_dims,
        n_group=n_group,
        label_rows=functools.partial(
            _label_rows, balltree_kwargs=dict(balltree_kwargs)
        ),
        name="balltree_group_id",
    )
