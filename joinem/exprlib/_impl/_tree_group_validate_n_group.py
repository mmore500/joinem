import numbers
import typing


def tree_group_validate_n_group(n_group: typing.Union[int, float]) -> None:
    """Raise ValueError unless `n_group` is a positive int or a float
    fraction in (0, 1]."""
    if isinstance(n_group, bool) or not isinstance(n_group, numbers.Real):
        raise ValueError(f"n_group must be int or float, got {n_group!r}")
    if isinstance(n_group, numbers.Integral):
        if n_group < 1:
            raise ValueError(f"int n_group must be positive, got {n_group}")
    elif not 0 < n_group <= 1:
        raise ValueError(
            f"float n_group must be a fraction in (0, 1], got {n_group}",
        )
