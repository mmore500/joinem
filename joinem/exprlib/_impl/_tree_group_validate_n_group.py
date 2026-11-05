import numbers


def tree_group_validate_n_group(n_group: int) -> None:
    """Raise ValueError unless `n_group` is a positive int."""
    if isinstance(n_group, bool) or not isinstance(n_group, numbers.Integral):
        raise ValueError(f"n_group must be int, got {n_group!r}")
    if n_group < 1:
        raise ValueError(f"n_group must be positive, got {n_group}")
