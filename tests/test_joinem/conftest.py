"""Configuration file for pytest."""

import random

import numpy as np
import pytest


# adapted from https://github.com/mmore500/hstrat/blob/e664e7ab3e8ea7aed0bbdd9ac7c6190bd6c83015/tests/test_hstrat/conftest.py#L44
@pytest.fixture(autouse=True)
def reseed_random(request: pytest.FixtureRequest) -> None:
    """Reseed random number generator to ensure determinstic test."""
    random.seed(request.node.name)
    np.random.seed(random.randint(1, 2**32 - 1))
