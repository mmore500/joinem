import numpy as np


def tree_group_label_tree_order(
    tree_order: np.ndarray, n_group: int
) -> np.ndarray:
    """Assign rows to `n_group` equal-sized (within one) groups, by cutting
    a tree-order permutation of row indices into contiguous chunks.

    Parameters
    ----------
    tree_order : np.ndarray
        Permutation of row indices in spatial tree traversal order, e.g.,
        `cKDTree.indices` or `BallTree.get_arrays()[1]`.
    n_group : int
        Number of groups, in [1, len(tree_order)] (or 1, if empty).

    Returns
    -------
    np.ndarray
        Group id in [0, n_group) for each row, as uint64.
    """
    n_row = len(tree_order)
    assert 1 <= n_group <= max(n_row, 1), (n_group, n_row)
    labels = np.empty(n_row, dtype=np.uint64)
    labels[tree_order] = np.arange(n_row, dtype=np.uint64) * n_group // n_row
    return labels
