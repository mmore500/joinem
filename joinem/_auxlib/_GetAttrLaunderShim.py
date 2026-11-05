import typing


class GetAttrLaunderShim:
    """A functor shim to launder the __module__ attribute of an object to a
    specified module name. This is used to hide implementation details in the
    documentation.

    See Also
    --------
    lazy_attach

    lazy_attach_stub
    """

    _wrapped_getattr: typing.Callable
    _should_launder: typing.Callable
    _module_name: str

    def __init__(
        self: "GetAttrLaunderShim",
        wrapped_getattr: typing.Callable,
        should_launder: typing.Callable,
        module_name: str,
    ) -> None:
        """Initialize functor."""
        self._wrapped_getattr = wrapped_getattr
        self._should_launder = should_launder
        self._module_name = module_name

    def __call__(
        self: "GetAttrLaunderShim", name: str, *args, **kwargs
    ) -> object:
        """Wraps the __getattr__ function to launder the __module__ attribute
        of returned objects."""
        attr = self._wrapped_getattr(name, *args, **kwargs)
        if self._should_launder(name):
            try:
                attr.__module__ = self._module_name
            except (AttributeError, TypeError):
                pass  # module attr not settable for all object types
        return attr
