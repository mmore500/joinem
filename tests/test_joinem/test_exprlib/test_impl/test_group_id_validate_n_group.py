import numpy as np
import pytest

from joinem.exprlib._impl import group_id_validate_n_group


@pytest.mark.parametrize("n_group", [1, 2, 1000, np.int64(3)])
def test_group_id_validate_n_group_valid(n_group):
    group_id_validate_n_group(n_group)


@pytest.mark.parametrize(
    "n_group",
    [0, -1, 0.0, 0.5, 1.0, 2.0, -0.5, 1.5, True, False, "2", None, [2]],
)
def test_group_id_validate_n_group_invalid(n_group):
    with pytest.raises(ValueError):
        group_id_validate_n_group(n_group)
