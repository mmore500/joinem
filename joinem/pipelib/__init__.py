"""Factories for frame-to-frame callables, suitable for `df.pipe(...)`.

Available within joinem `--ipipe` and `--opipe` expressions as `jepl`.
"""

from ._PipeUnaryOpsOp import PipeUnaryOpsOp
from ._SampleOp import SampleOp
from ._pipe_unary_ops import pipe_unary_ops

__all__ = ["pipe_unary_ops", "PipeUnaryOpsOp", "SampleOp"]
