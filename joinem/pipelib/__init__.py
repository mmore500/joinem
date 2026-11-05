"""Factories for frame-to-frame callables, suitable for `df.pipe(...)`.

Available within joinem `--ipipe` and `--opipe` expressions as `jepl`.
"""

from ._SampleOp import SampleOp

__all__ = ["SampleOp"]
