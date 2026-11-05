import itertools as it

import numpy as np
import pytest

from joinem.exprlib._impl import label_tree_order


def test_label_tree_order_identity():
    labels = label_tree_order(np.arange(10), 5)
    assert labels.dtype == np.uint64
    assert labels.tolist() == [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]


def test_label_tree_order_permuted():
    tree_order = np.array([3, 0, 2, 1])
    labels = label_tree_order(tree_order, 2)
    # rows 3, 0 come first in tree order, so form group 0
    assert labels.tolist() == [0, 1, 1, 0]


@pytest.mark.parametrize(
    "n_row, n_group",
    [
        (n_row, n_group)
        for n_row, n_group in it.product(
            [1, 2, 7, 100, 1001], [1, 2, 3, 7, 100]
        )
        if n_group <= n_row
    ],
)
def test_label_tree_order_balanced(n_row: int, n_group: int):
    labels = label_tree_order(np.random.permutation(n_row), n_group)
    counts = np.bincount(labels.astype(np.int64))
    assert len(counts) == n_group
    assert counts.max() - counts.min() <= 1


def test_label_tree_order_contiguous():
    tree_order = np.random.permutation(100)
    labels = label_tree_order(tree_order, 7)
    # labels are non-decreasing along tree order
    assert (np.diff(labels[tree_order].astype(np.int64)) >= 0).all()


def test_label_tree_order_empty():
    assert len(label_tree_order(np.array([], dtype=np.int64), 1)) == 0
