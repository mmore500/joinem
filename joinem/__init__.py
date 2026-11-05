__author__ = "Matthew Andres Moreno"
__copyright__ = "Copyright 2024, Matthew Andres Moreno"
__credits__ = ["Matthew Andres Moreno"]
__license__ = "MIT"
__version__ = "0.18.0"
__maintainer__ = "Matthew Andres Moreno"
__email__ = "m.more500@gmail.com"

from ._auxlib import lazy_attach_stub

__getattr__, __dir__, __all__ = lazy_attach_stub(
    __name__,
    __file__,
    should_launder=[].__contains__,
)
del lazy_attach_stub
