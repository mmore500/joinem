import warnings

import numpy as np
import pytest

from joinem.exprlib._impl import group_id_resolve_n_group


@pytest.mark.parametrize(
    "n_group, n_row, expected",
    [
        (1, 10, 1),
        (5, 10, 5),
        (10, 10, 10),
        (np.int64(3), 10, 3),
    ],
)
def test_group_id_resolve_n_group(n_group, n_row: int, expected: int):
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # no clipping warning expected
        res = group_id_resolve_n_group(n_group, n_row)
    assert res == expected
    assert isinstance(res, int)


@pytest.mark.parametrize("n_group", [11, 1000, np.int64(11)])
def test_group_id_resolve_n_group_clip_warns(n_group):
    with pytest.warns(UserWarning, match="clipping to 10 groups"):
        assert group_id_resolve_n_group(n_group, 10) == 10
