import functools
import types
import typing

import numpy as np
import polars as pl

from ._tree_group_id import _partition_frontier, _tree_group_id


def _label_rows(
    X: np.ndarray, n_group: int, balltree_kwargs: typing.Mapping
) -> np.ndarray:
    from sklearn.neighbors import BallTree  # deferred, slow import

    n_row = len(X)
    if n_group >= n_row:
        return np.arange(n_row)

    # sklearn ball trees are complete binary trees, so with small enough
    # leaves there are at least n_group leaves (barring n_group ~ n_row)
    kwargs = {"leaf_size": max(1, (n_row - 1) // (2 * n_group))}
    kwargs.update(balltree_kwargs)
    __, idx_array, node_data, __ = BallTree(X, **kwargs).get_arrays()
    n_node = len(node_data)

    def children(i: int) -> typing.List[int]:
        return [c for c in (2 * i + 1, 2 * i + 2) if c < n_node]

    frontier = _partition_frontier(
        0,
        n_group,
        size=lambda i: node_data[i]["idx_end"] - node_data[i]["idx_start"],
        start=lambda i: node_data[i]["idx_start"],
        children=children,
    )
    labels = np.empty(n_row, dtype=np.uint32)
    for group, i in enumerate(frontier):
        rows = idx_array[node_data[i]["idx_start"] : node_data[i]["idx_end"]]
        labels[rows] = group
    return labels


def balltree_group_id(
    on_dims: typing.Sequence[typing.Union[pl.Expr, str]],
    n_group: typing.Union[int, float],
    balltree_kwargs: typing.Mapping[str, typing.Any] = types.MappingProxyType(
        {}
    ),
) -> pl.Expr:
    """Partition rows into spatially compact groups via a ball tree.

    Builds a `sklearn.neighbors.BallTree` over `on_dims` and descends from
    the root, splitting the most populous node until `n_group` nodes
    partition the rows. Evaluates to each row's group id (UInt32, numbered
    in tree order). Deterministic; no seed needed.

    Parameters
    ----------
    on_dims : sequence of str or pl.Expr
        Coordinates for each row; distance is Euclidean by default. Consider scaling
        with `standard_scaled` or `minmax_scaled` first. Rows with any
        null/NaN coordinate get a null group id.
    n_group : int or float
        Number of groups (int), or fraction of non-null rows (float in
        (0, 1]). At most this many groups result: fewer if there are fewer
        rows, or if tree leaves run out (e.g., when `n_group` approaches
        the row count, or `leaf_size` is overridden to be large).
    balltree_kwargs : mapping, optional
        Additional keyword arguments for `sklearn.neighbors.BallTree`, such
        as `metric`. Overrides the default `leaf_size`, which is chosen to
        give at least `n_group` leaves.

    Raises
    ------
    ValueError
        If `on_dims` is empty, or `n_group` is non-positive or a float
        outside (0, 1].

    Notes
    -----
    Groups are computed over all rows in the evaluation context; within
    `.over(...)`, groups are computed separately per window.

    Examples
    --------
    Thin to ~100 spatially spread rows, one per group:

    >>> jepl.SampleOp(1, stratify=jexl.balltree_group_id(["x", "y"], 100))
    """
    label_rows = functools.partial(
        _label_rows, balltree_kwargs=dict(balltree_kwargs)
    )
    return _tree_group_id(on_dims, n_group, label_rows, "balltree_group_id")
