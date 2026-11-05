import numpy as np
import pytest

from joinem.exprlib._impl import tree_group_validate_n_group


@pytest.mark.parametrize("n_group", [1, 2, 1000, np.int64(3), 0.5, 1.0, 1e-9])
def test_validate_n_group_valid(n_group):
    tree_group_validate_n_group(n_group)


@pytest.mark.parametrize(
    "n_group", [0, -1, 0.0, -0.5, 1.5, True, False, "2", None, [2]]
)
def test_validate_n_group_invalid(n_group):
    with pytest.raises(ValueError):
        tree_group_validate_n_group(n_group)
