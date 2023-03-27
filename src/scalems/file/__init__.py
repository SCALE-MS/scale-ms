"""Provide an interface for managing filesystem objects in a workflow.

We distinguish between *File* and *FileReference* types.

A *File* is a concrete representation of a local filesystem object.

A *FileReference* is more general. It may represent a non-local object
(accessible by a URI). We currently use *FileReference* objects exclusively for
resources managed by scalems. (See `scalems.store.FileStore`.)

This module has minimal dependencies on other scalems implementation details so
that it can be included easily by scalems modules without circular dependencies.
"""

__all__ = (
    "AbstractFileReference",
    "AbstractFile",
    "AccessFlags",
    "BaseBinary",
    "BaseText",
    "DataLocalizationError",
    "describe_file",
)

import abc
import codecs
import enum
import hashlib
import locale
import mmap
import os
import pathlib
import typing

import scalems.exceptions as _exceptions
from .. import identifiers as _identifiers


class DataLocalizationError(_exceptions.ScopeError):
    """The requested file operation is not possible in the given context.

    This may be because a referenced file exists in a different FileStore than the
    current default. Note that data localization is a potentially expensive operation
    and so must be explicitly requested by the user.
    """


@typing.runtime_checkable
class AbstractFileReference(typing.Protocol):
    """Reference to a managed file.

    A FileReference is an :py:class:`os.PathLike` object.

    If *filereference.is_local()* is ``True``, :py:func:`os.fspath()` will return a
    :py:class:`str` for the referenced file's path in the local filesystem.
    If the file is not available locally, an exception will be raised.

    Example:
        handle: scalems.file.AbstractFileReference = filestore.add_file(...)
    """

    @abc.abstractmethod
    def __fspath__(self) -> str:
        """Represent the local filesystem path, if possible.

        Support `os.PathLike`.

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
    async def localize(self, context=None) -> "AbstractFileReference":
        """Make sure that the referenced file is available locally for a given context.

        With no arguments, *localize()* affects the currently active FileStore.
        Specific (e.g. remote) data stores can be addressed by providing *context*.

        If the file is not yet localized, yields until the necessary data transfers
        have completed.

        Raises:
            TBD

        TODO:
            Abstract type for required *context* interface.
        """
        return self

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
        if context is None and self.is_local():
            return pathlib.Path(self).as_uri()
        else:
            raise _exceptions.MissingImplementationError(
                "Protocol for non-local filesystem contexts is a work in progress."
            )  # return self.path(context=context).as_uri()

    @abc.abstractmethod
    def filestore(self):
        """Get a handle to the managing filestore for this reference."""
        raise NotImplementedError

    @abc.abstractmethod
    def key(self) -> _identifiers.ResourceIdentifier:
        """Get the identifying key for the file reference.

        The return value may not be human-readable, but may be necessary for locating
        the referenced file through the filestore.
        """
        raise NotImplementedError


class AccessFlags(enum.Flag):
    NO_ACCESS = 0
    READ = enum.auto()
    WRITE = enum.auto()


class AbstractFile(typing.Protocol):
    """Abstract base for file types.

    Extends `os.PathLike` with the promise of a
    :py:func:`~AbstractFile.fingerprint()` method and some support for access
    control semantics.

    See :py:func:`describe_file` for a creation function.
    """

    access: AccessFlags
    """Declared access requirements.

    Not yet enforced or widely used.
    """

    @abc.abstractmethod
    def __fspath__(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def fingerprint(self) -> bytes:
        """Get a checksum or other suitable fingerprint for the file data.

        Note that the fingerprint could change if the file is written to during
        the lifetime of the handle. This is too complicated to optimize, generally,
        so possible caching of fingerprint results is left to the caller.
        """
        raise NotImplementedError


class BaseBinary(AbstractFile):
    """Base class for binary file types."""

    _path: str

    def __init__(self, path: typing.Union[str, pathlib.Path, os.PathLike], access: AccessFlags = AccessFlags.READ):
        self._path = str(pathlib.Path(path).resolve())
        self.access = access

    def __fspath__(self) -> str:
        return self._path

    def fingerprint(self) -> bytes:
        with open(os.fspath(self), "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as data:
                m = hashlib.sha256(data)
        checksum: bytes = m.digest()
        return checksum


class BaseText(BaseBinary):
    """Base class for text file types."""

    encoding = locale.getpreferredencoding(False)

    def __init__(self, path, access: AccessFlags = AccessFlags.READ, encoding=None):
        if encoding is None:
            encoding = locale.getpreferredencoding(False)
        try:
            codecs.getencoder(encoding)
        except LookupError:
            raise ValueError("Specify a valid character encoding for text files.")
        self.encoding = encoding
        super().__init__(path=path, access=access)

    def fingerprint(self) -> bytes:
        m = hashlib.sha256()
        # Use universal newlines and utf-8 encoding for text file fingerprinting.
        with open(self._path, "r", encoding=self.encoding) as f:
            # If we knew more about the text file size or line length, or if we already
            # had an open buffer, socket, or mmap, we could  optimize this in subclasses.
            for line in f:
                m.update(line.encode("utf8"))
        checksum: bytes = m.digest()
        return checksum


def describe_file(
    obj: typing.Union[str, pathlib.Path, os.PathLike], mode="rb", encoding: str = None, file_type_hint=None
) -> AbstractFile:
    """Describe an existing local file."""
    access = AccessFlags.NO_ACCESS
    if "r" in mode:
        access |= AccessFlags.READ
        if "w" in mode:
            access |= AccessFlags.WRITE
    else:
        # If neither 'r' nor 'w' is provided, still assume 'w'
        access |= AccessFlags.WRITE
    # TODO: Confirm requested access permissions.

    if file_type_hint:
        raise _exceptions.MissingImplementationError("Extensible file types not yet supported.")

    if "b" in mode:
        if encoding is not None:
            raise TypeError("*encoding* argument is not supported for binary files.")
        file = BaseBinary(path=obj, access=access)
    else:
        file = BaseText(path=obj, access=access, encoding=encoding)

    if not pathlib.Path(file).exists():
        raise ValueError("Not an existing file.")

    return file
