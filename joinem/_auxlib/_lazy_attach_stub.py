from typing import Any, Callable, List, Tuple

import lazy_loader

from ._GetAttrLaunderShim import GetAttrLaunderShim


# adapted from https://github.com/mmore500/hstrat/blob/e664e7ab3e8ea7aed0bbdd9ac7c6190bd6c83015/hstrat/_auxiliary_lib/_lazy_attach_stub.py#L8
def lazy_attach_stub(
    module_name: str,
    module_path: str,
    *,
    should_launder: Callable = lambda _: True,
) -> Tuple[Callable[[str], Any], Callable[[], List[str]], List[str]]:
    """Attach a lazy loading stub to a package, deferring loading of
    submodules and attributes until access to improve load times.

    Symbols to import are declared in a type stub (an `__init__.pyi` file)
    alongside the package's `__init__.py`, which calls this function.

    Parameters
    ----------
    module_name : str
        The name of the package in which symbols are being imported. This
        should be `__name__`.
    module_path : str
        The file path of the package's `__init__.py`. This should be
        `__file__`.
    should_launder : callable, default `lambda x: True`
        A callable that takes a string attr and returns a boolean indicating
        whether the attr should be laundered. If True, __module__ on the
        attr will attempt to be set to `module_name`.

    Returns
    -------
    tuple
        Items needed to define `__getattr__`, `__dir__`, and `__all__` in
        the package.

    Notes
    -----
    See the Scientific Python lazy loading spec,
    https://scientific-python.org/specs/spec-0001/.
    """
    getattr__, dir__, all__ = lazy_loader.attach_stub(module_name, module_path)
    wrapped_getattr = GetAttrLaunderShim(
        getattr__, should_launder, module_name
    )
    return wrapped_getattr, dir__, all__
