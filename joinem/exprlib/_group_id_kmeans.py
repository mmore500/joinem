import functools
import types
import typing
import warnings

import numpy as np
import polars as pl
from scipy.cluster.vq import kmeans2

from ._impl import group_id_make_expr


def _label_rows(
    X: np.ndarray,
    n_group: int,
    seed: typing.Optional[int],
    kmeans_kwargs: typing.Mapping,
) -> np.ndarray:
    kwargs = {"minit": "++", **kmeans_kwargs}
    with warnings.catch_warnings():
        # empty clusters are reported once below instead
        warnings.simplefilter("ignore")
        __, labels = kmeans2(X.astype(float), n_group, rng=seed, **kwargs)
    __, labels = np.unique(labels, return_inverse=True)  # drop empty ids
    n_found = labels.max() + 1
    if n_found < n_group:
        warnings.warn(
            f"k-means found only {n_found} of n_group={n_group} groups, "
            "as some clusters came out empty",
        )
    return labels


def group_id_kmeans(
    on_dims: typing.Sequence[typing.Union[pl.Expr, str]],
    *,
    n_group: int,
    seed: typing.Optional[int] = None,
    kmeans_kwargs: typing.Mapping[str, typing.Any] = types.MappingProxyType(
        {}
    ),
) -> pl.Expr:
    """Get a distinct integer ID for each spatial cluster of rows,
    partitioned via k-means.

    The ID starts at 0 and increases by one for each group. Rows with any
    null or NaN coordinate get a null ID.

    Parameters
    ----------
    on_dims : sequence of str or pl.Expr
        Coordinates for each row; distance is Euclidean. Consider scaling
        with `scaled_standard` or `scaled_minmax` first.
    n_group : int
        Number of clusters. Clipped, with a warning, to the number of
        non-null rows.
    seed : int, optional
        Seed for cluster initialization. If None, groups vary across runs.
    kmeans_kwargs : mapping, optional
        Additional keyword arguments for `scipy.cluster.vq.kmeans2`, such
        as `iter`. Overrides default `minit="++"`.

    Returns
    -------
    pl.Expr
        Expression of data type `UInt64`, named "group_id_kmeans".

    Raises
    ------
    ValueError
        If `on_dims` is empty, or `n_group` is not a positive int.

    See Also
    --------
    group_id_kdtree :
        Faster and deterministic, with equal-sized groups that follow
        where rows are dense.

    Notes
    -----
    Clusters cover space rather than rows: sparse regions get groups of
    their own, so group sizes are uneven. Fewer than `n_group` groups
    result, with a warning, if some clusters come out empty (e.g., with
    few distinct points).

    Groups are computed over all rows in the evaluation context; within
    `.over(...)`, groups are computed separately per window.

    Examples
    --------
    Sample two rows from each of 100 clusters, spreading the sample
    evenly over space.

    >>> SampleOp(2, stratify=group_id_kmeans(["x", "y"], n_group=100, seed=1))
    """
    return group_id_make_expr(
        on_dims,
        n_group=n_group,
        label_rows=functools.partial(
            _label_rows, seed=seed, kmeans_kwargs=dict(kmeans_kwargs)
        ),
        name="group_id_kmeans",
    )
