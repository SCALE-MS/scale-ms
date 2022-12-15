"""Provide an interface for managing filesystem objects in a workflow."""

__all__ = ["FileReference", "DataLocalizationError"]

import abc
import pathlib
import typing

from scalems.exceptions import ScopeError


class DataLocalizationError(ScopeError):
    """The requested file operation is not possible in the given context.

    This may be because a referenced file exists in a different FileStore than the
    current default. Note that data localization is a potentially expensive operation
    and so must be explicitly requested by the user.
    """


@typing.runtime_checkable
class FileReference(typing.Protocol):
    """Reference to a managed file.

    A FileReference is an :py:class:`os.PathLike` object.

    If *filereference.is_local()* is ``True``, :py:func:`os.fspath()` will return a
    :py:class:`str` for the referenced file's path in the local filesystem.
    If the file is not available locally, an exception will be raised.

    Example:
        handle: scalems.FileReference = filestore.add_file(...)
    """

    @abc.abstractmethod
    def __fspath__(self) -> str:
        """Represent the local filesystem path, if possible.

        Returns:
            str: representation of the filesystem path if the referenced file is
            available locally.

        Raises:
            DataLocalizationError: if the file has not been transferred to the local
            file store.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def is_local(self, context=None) -> bool:
        """Check for file existence in a given context.

        By default, file existence is checked in the currently active FileStore.
        If *context* is specified, checks whether the referenced file is locally
        available in the given context.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def localize(self, context=None) -> "FileReference":
        """Make sure that the referenced file is available locally for a given context.

        With no arguments, *localize()* affects the currently active FileStore.
        Specific (e.g. remote) data stores can be addressed by providing *context*.

        If the file is not yet localized, yields until the necessary data transfers
        have completed.

        Raises:
            TBD
        """
        return self

    def path(self, context=None) -> pathlib.Path:
        """Get the path for the referenced file.

        If *context* is specified, the returned :py:class:`~pathlib.Path` is valid in
        the filesystem associated with the given context.

        If *context* is unspecified, the filesystem for the currently active (client)
        FileStore is used.

        Raises:
            DataLocalizationError: if the referenced file has not been localized.
        """
        return context.files[self.key()]

    def as_uri(self, context=None) -> str:
        """Get an appropriate URI for a referenced file, whether or not it is available.

        For local file stores, the result is the same as for *obj.path().as_uri()* except
        that no exception is raised if the object does not yet exist locally.

        When *context* is specified, the returned URI may not be resolvable without
        additional contextual facilities. For instance, :py:mod:`scalems.radical`
        contexts may return a :py:class:`radical.saga.Url` using one of the scheme
        extensions for RADICAL Pilot
        :py:mod:`staging directives <radical.pilot.staging_directives>`.
        """
        return self.path(context=context).as_uri()

    @abc.abstractmethod
    def filestore(self):
        """Get a handle to the managing filestore for this reference."""
        raise NotImplementedError

    @abc.abstractmethod
    def key(self):
        """Get the identifying key for the file reference.

        The return value may not be human readable, but may be necessary for locating
        the referenced file through the filestore.
        """
        raise NotImplementedError
