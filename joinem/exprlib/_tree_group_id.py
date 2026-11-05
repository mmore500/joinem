import heapq
import math
import numbers
import typing

import numpy as np
import polars as pl

# (count, start, children) -> nested nodes; see _partition_frontier
_Node = typing.TypeVar("_Node")


def _validate_n_group(n_group: typing.Union[int, float]) -> None:
    if isinstance(n_group, bool) or not isinstance(n_group, numbers.Real):
        raise ValueError(f"n_group must be int or float, got {n_group!r}")
    if isinstance(n_group, numbers.Integral):
        if n_group < 1:
            raise ValueError(f"int n_group must be positive, got {n_group}")
    elif not 0 < n_group <= 1:
        raise ValueError(
            f"float n_group must be a fraction in (0, 1], got {n_group}",
        )


def _resolve_n_group(n_group: typing.Union[int, float], n_row: int) -> int:
    if isinstance(n_group, numbers.Integral):
        return min(int(n_group), n_row)
    return max(1, min(math.floor(n_group * n_row), n_row))


def _partition_frontier(
    root: _Node,
    n_group: int,
    size: typing.Callable[[_Node], int],
    start: typing.Callable[[_Node], int],
    children: typing.Callable[[_Node], typing.Sequence[_Node]],
) -> typing.List[_Node]:
    """Descend from `root`, repeatedly splitting the largest splittable
    node, until `n_group` nodes partition the data or no node can split.

    Returned nodes are ordered by `start`, i.e., tree traversal order.
    """
    # heap entries: (-size, start, tiebreak, node); tiebreak avoids
    # comparing nodes, which may not be orderable
    tiebreak = 0
    heap = [(-size(root), start(root), tiebreak, root)]
    done = []
    while heap and len(heap) + len(done) < n_group:
        __, __, __, node = heapq.heappop(heap)
        kids = children(node)
        if not kids:
            done.append(node)
            continue
        for kid in kids:
            tiebreak += 1
            heapq.heappush(heap, (-size(kid), start(kid), tiebreak, kid))
    return sorted([*done, *(entry[-1] for entry in heap)], key=start)


def _tree_group_id(
    on_dims: typing.Sequence[typing.Union[pl.Expr, str]],
    n_group: typing.Union[int, float],
    label_rows: typing.Callable[[np.ndarray, int], np.ndarray],
    name: str,
) -> pl.Expr:
    """Shared implementation for spatial-tree group id expressions.

    `label_rows(X, n_group)` must return one group id per row of `X`.
    """
    if isinstance(on_dims, (str, pl.Expr)):
        on_dims = [on_dims]
    on_dims = list(on_dims)
    if not on_dims:
        raise ValueError("on_dims must contain at least one dimension")
    if not all(isinstance(dim, (str, pl.Expr)) for dim in on_dims):
        raise ValueError(f"on_dims must be str or pl.Expr, got {on_dims!r}")
    _validate_n_group(n_group)

    dims = [
        (pl.col(dim) if isinstance(dim, str) else dim)
        .cast(pl.Float64)
        .alias(f"_{i}")  # avoid struct field name collisions
        for i, dim in enumerate(on_dims)
    ]

    def apply(series: pl.Series) -> pl.Series:
        X = series.struct.unnest().to_numpy().astype(np.float64)
        valid = ~np.isnan(X).any(axis=1)  # nulls become nan
        result = np.zeros(len(X), dtype=np.uint32)
        n_row = int(valid.sum())
        if n_row:
            result[valid] = label_rows(
                X[valid], _resolve_n_group(n_group, n_row)
            )
        return pl.Series(name, result, dtype=pl.UInt32).set(
            pl.Series(~valid), None
        )

    return (
        pl.struct(dims).map_batches(apply, return_dtype=pl.UInt32).alias(name)
    )
