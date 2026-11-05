import math
import numbers
import typing

import numpy as np


def resolve_n_group(n_group: typing.Union[int, float], n_row: int) -> int:
    """Convert `n_group`, an int count or float fraction of `n_row`, to a
    group count clipped to [1, n_row].

    Assumes `n_group` passed `validate_n_group` and `n_row` is positive.
    """
    if not isinstance(n_group, numbers.Integral):
        n_group = math.floor(n_group * n_row)
    return int(np.clip(n_group, 1, n_row))
