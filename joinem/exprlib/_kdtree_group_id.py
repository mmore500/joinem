import functools
import types
import typing

import numpy as np
import polars as pl
from scipy.spatial import cKDTree

from ._impl import tree_group_label_tree_order, tree_group_make_id_expr


def _label_rows(
    X: np.ndarray, n_group: int, kdtree_kwargs: typing.Mapping
) -> np.ndarray:
    kwargs = {"balanced_tree": True, **kdtree_kwargs}
    tree = cKDTree(X, leafsize=1, **kwargs)  # leaves are unordered
    return tree_group_label_tree_order(tree.indices, n_group)


def kdtree_group_id(
    on_dims: typing.Sequence[typing.Union[pl.Expr, str]],
    *,
    n_group: typing.Union[int, float],
    kdtree_kwargs: typing.Mapping[str, typing.Any] = types.MappingProxyType(
        {}
    ),
) -> pl.Expr:
    """Get a distinct integer ID for each spatially compact group of rows,
    partitioned via a k-d tree.

    The ID starts at 0 and increases by one for each group, up to
    `n_group - 1`. Rows with any null or NaN coordinate get a null ID.

    Parameters
    ----------
    on_dims : sequence of str or pl.Expr
        Coordinates for each row. Consider scaling with `standard_scaled`
        or `minmax_scaled` first.
    n_group : int or float
        Number of groups (int), or fraction of non-null rows (float in
        (0, 1]). Clipped to the number of non-null rows.
    kdtree_kwargs : mapping, optional
        Additional keyword arguments for `scipy.spatial.cKDTree`. Overrides
        default `balanced_tree=True`. Passing `leafsize` raises TypeError,
        as `leafsize=1` is required for spatially compact groups.

    Returns
    -------
    pl.Expr
        Expression of data type `UInt64`, named "kdtree_group_id".

    Raises
    ------
    ValueError
        If `on_dims` is empty, or `n_group` is non-positive or a float
        outside (0, 1].

    See Also
    --------
    balltree_group_id :
        Ball tree counterpart, with ball-shaped rather than box-shaped
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

    >>> SampleOp(2, stratify=kdtree_group_id(["x", "y"], n_group=100))
    """
    return tree_group_make_id_expr(
        on_dims,
        n_group=n_group,
        label_rows=functools.partial(
            _label_rows, kdtree_kwargs=dict(kdtree_kwargs)
        ),
        name="kdtree_group_id",
    )
