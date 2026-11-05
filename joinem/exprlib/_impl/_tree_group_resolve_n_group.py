import math
import numbers
import typing
import warnings

import numpy as np


def tree_group_resolve_n_group(
    n_group: typing.Union[int, float], n_row: int
) -> int:
    """Convert `n_group`, an int count or float fraction of `n_row`, to a
    group count clipped to [1, n_row].

    Warns if an int `n_group` exceeds `n_row`. Assumes `n_group` passed
    `tree_group_validate_n_group` and `n_row` is positive.
    """
    if not isinstance(n_group, numbers.Integral):
        n_group = math.floor(n_group * n_row)
    elif n_group > n_row:
        warnings.warn(
            f"n_group={n_group} exceeds the {n_row} non-null rows available, "
            f"clipping to {n_row} groups",
        )
    return int(np.clip(n_group, 1, n_row))
