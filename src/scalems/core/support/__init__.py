"""Implementation details and supporting utilities."""


__all__ = []


class _PrototypicalDescriptor:
    """A prototypical Descriptor class for illustration / quick reference."""

    # Ref: https://docs.python.org/3/reference/datamodel.html#implementing-descriptors
    def __set_name__(self, owner, name):
        # Called by type.__new__ during class creation to allow customization.
        ...

    def __get__(self, instance, owner):
        # Note that instance==None when called through the *owner* (as a class attribute).
        ...

    def __set__(self, instance, value):
        # If defined, the descriptor is a Data Descriptor and will not be overridden in instances.
        ...

    def __delete__(self, instance):
        # Optional method for Data Descriptors.
        ...
