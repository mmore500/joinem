import numpy as np
import pytest

from joinem.exprlib._impl import tree_group_resolve_n_group


@pytest.mark.parametrize(
    "n_group, n_row, expected",
    [
        (1, 10, 1),
        (5, 10, 5),
        (10, 10, 10),
        (11, 10, 10),
        (np.int64(3), 10, 3),
        (0.5, 10, 5),
        (0.55, 10, 5),  # floors, mirroring polars sample(fraction=...)
        (1.0, 10, 10),
        (0.01, 10, 1),  # clipped up to at least one group
        (0.5, 1, 1),
    ],
)
def test_resolve_n_group(n_group, n_row: int, expected: int):
    res = tree_group_resolve_n_group(n_group, n_row)
    assert res == expected
    assert isinstance(res, int)
