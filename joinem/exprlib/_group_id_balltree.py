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
    # pinned, not overridable: larger leaves scramble groups spatially
    tree = BallTree(X, leaf_size=1, **balltree_kwargs)
    __, tree_order, __, __ = tree.get_arrays()
    return tree_group_label_tree_order(tree_order, n_group)


def group_id_balltree(
    on_dims: typing.Sequence[typing.Union[pl.Expr, str]],
    *,
    n_group: int,
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
        Consider scaling with `scaled_standard` or `scaled_minmax` first.
    n_group : int
        Number of groups. Clipped, with a warning, to the number of
        non-null rows.
    balltree_kwargs : mapping, optional
        Additional keyword arguments for `sklearn.neighbors.BallTree`, such
        as `metric`. Must not contain `leaf_size`, as `leaf_size=1` is
        required for spatially compact groups.

    Returns
    -------
    pl.Expr
        Expression of data type `UInt64`, named "group_id_balltree".

    Raises
    ------
    ValueError
        If `on_dims` is empty, or `n_group` is not a positive int.

    See Also
    --------
    group_id_kdtree :
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
    Sample two rows from each of 100 groups of clustered rows.

    >>> SampleOp(2, stratify=group_id_balltree(["x", "y"], n_group=100))
    """
    return tree_group_make_id_expr(
        on_dims,
        n_group=n_group,
        label_rows=functools.partial(
            _label_rows, balltree_kwargs=dict(balltree_kwargs)
        ),
        name="group_id_balltree",
    )
