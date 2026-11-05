import functools
import types
import typing

import numpy as np
import polars as pl

from ._tree_group_id import _partition_frontier, _tree_group_id


def _label_rows(
    X: np.ndarray, n_group: int, kdtree_kwargs: typing.Mapping
) -> np.ndarray:
    from scipy.spatial import cKDTree  # deferred, slow import

    n_row = len(X)
    if n_group >= n_row:
        return np.arange(n_row)

    kwargs = {"leafsize": 1, "balanced_tree": True}
    kwargs.update(kdtree_kwargs)
    tree = cKDTree(X, **kwargs)
    frontier = _partition_frontier(
        tree.tree,
        n_group,
        size=lambda node: node.children,
        start=lambda node: node.start_idx,
        children=lambda node: (
            [] if node.lesser is None else [node.lesser, node.greater]
        ),
    )
    labels = np.empty(n_row, dtype=np.uint32)
    for group, node in enumerate(frontier):
        labels[node.indices] = group
    return labels


def kdtree_group_id(
    on_dims: typing.Sequence[typing.Union[pl.Expr, str]],
    n_group: typing.Union[int, float],
    kdtree_kwargs: typing.Mapping[str, typing.Any] = types.MappingProxyType(
        {}
    ),
) -> pl.Expr:
    """Partition rows into spatially compact groups via a k-d tree.

    Builds a `scipy.spatial.cKDTree` (balanced, by default) over `on_dims` and descends
    from the root, splitting the most populous node until `n_group` nodes
    partition the rows. Evaluates to each row's group id (UInt32, numbered
    in tree order). Groups are axis-aligned boxes, whereas
    `balltree_group_id` gives ball-shaped groups. Deterministic; no seed
    needed.

    Parameters
    ----------
    on_dims : sequence of str or pl.Expr
        Coordinates for each row. Consider scaling with `standard_scaled`
        or `minmax_scaled` first. Rows with any null/NaN coordinate get a
        null group id.
    n_group : int or float
        Number of groups (int), or fraction of non-null rows (float in
        (0, 1]). At most this many groups result: fewer if there are fewer
        rows, or if duplicate points or large leaves prevent further
        splits.
    kdtree_kwargs : mapping, optional
        Additional keyword arguments for `scipy.spatial.cKDTree`. Overrides
        defaults `leafsize=1` and `balanced_tree=True`.

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

    >>> jepl.SampleOp(1, stratify=jexl.kdtree_group_id(["x", "y"], 100))
    """
    label_rows = functools.partial(
        _label_rows, kdtree_kwargs=dict(kdtree_kwargs)
    )
    return _tree_group_id(on_dims, n_group, label_rows, "kdtree_group_id")
