import warnings


def group_id_resolve_n_group(n_group: int, n_row: int) -> int:
    """Clip `n_group` to `n_row`, warning if clipping occurs.

    Assumes `n_group` passed `group_id_validate_n_group` and `n_row` is
    positive.
    """
    if n_group > n_row:
        warnings.warn(
            f"n_group={n_group} exceeds the {n_row} non-null rows available, "
            f"clipping to {n_row} groups",
        )
        return n_row
    return int(n_group)
