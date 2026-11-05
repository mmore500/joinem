import numpy as np
import polars as pl
import pytest

from joinem.exprlib._impl import tree_group_id


def _label_rows(X: np.ndarray, n_group: int) -> np.ndarray:
    # sort by first coordinate as a stand-in for a spatial tree
    return np.argsort(np.argsort(X[:, 0])) * n_group // len(X)


def test_tree_group_id():
    df = pl.DataFrame({"x": [3.0, 1.0, 2.0, 0.0]})
    res = df.select(
        tree_group_id(["x"], n_group=2, label_rows=_label_rows, name="gid"),
    ).to_series()
    assert res.name == "gid"
    assert res.dtype == pl.UInt64
    assert res.to_list() == [1, 0, 1, 0]


def test_tree_group_id_passes_valid_rows_only():
    seen = []

    def label_rows(X: np.ndarray, n_group: int) -> np.ndarray:
        seen.append((X.tolist(), n_group))
        return np.zeros(len(X), dtype=np.uint64)

    df = pl.DataFrame({"x": [1.0, None, float("nan"), 4.0], "y": [1, 2, 3, 4]})
    res = df.select(
        tree_group_id(
            ["x", "y"], n_group=5, label_rows=label_rows, name="gid"
        ),
    ).to_series()
    assert seen == [([[1.0, 1.0], [4.0, 4.0]], 2)]  # n_group clipped
    assert res.to_list() == [0, None, None, 0]


def test_tree_group_id_same_named_dims():
    df = pl.DataFrame({"x": [3.0, 1.0, 2.0, 0.0]})
    res = df.select(
        tree_group_id(
            [pl.col("x"), pl.col("x") * 2],
            n_group=2,
            label_rows=_label_rows,
            name="gid",
        ),
    ).to_series()
    assert res.to_list() == [1, 0, 1, 0]


@pytest.mark.parametrize("on_dims", [[], [1], [None], ["x", 2.0]])
def test_tree_group_id_invalid_dims(on_dims):
    with pytest.raises(ValueError):
        tree_group_id(on_dims, n_group=2, label_rows=_label_rows, name="g")


def test_tree_group_id_invalid_n_group():
    with pytest.raises(ValueError):
        tree_group_id(["x"], n_group=0, label_rows=_label_rows, name="g")
