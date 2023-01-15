"""Locking protocol for scalems data stores."""

__all__ = ["LockException", "is_locked", "scoped_directory_lock"]

import contextlib
import logging
import os
import pathlib
import typing
import warnings

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

_lock_directory_name = ".scalems_lock"
"""Name to use for lock directories (module constant)"""


class LockException(Exception):
    """The requested lock could not be obtained."""


def is_locked(path: pathlib.Path):
    """Look for evidence of a scalems lock without attempting a lock."""
    lock_names = (_lock_directory_name,)
    for name in lock_names:
        if path.joinpath(name).exists():
            return True
    return False


def _lock_directory(path=None):
    """Provide a fall-back mechanism for locking a directory.

    If we find better ways to "lock" directories in certain environments, we can decide
    how to dispatch alternative locking mechanisms. This implementation should be fairly
    robust, though, and can always serve as a fall-back.

    Locks must be explicitly removed by calling _unlock_directory(path). (Use
    `scoped_directory_lock()` for a context manager.)

    Note that, while this function should be thread-safe, the `_unlock_directory()`
    function cannot be. Care should be taken not to call `_unlock_directory()` more than
    once for a given token.

    Raises:
        LockException if the lock could not be obtained.

    """
    if path is None:
        path = os.getcwd()
    path = pathlib.Path(path).resolve()
    token_path = path.joinpath(_lock_directory_name)
    try:
        token_path.mkdir()
    except FileExistsError as e:
        # Directory lock already exists.
        raise LockException("{} already exists".format(token_path)) from e
    return token_path


def _unlock_directory(path=None):
    """Remove a lock from a directory path.

    This must be called to release a lock established with _lock_directory(path).
    """
    if path is None:
        path = os.getcwd()
    token_path = pathlib.Path(path) / _lock_directory_name
    assert token_path.exists()
    token_path.rmdir()


class _Lock:
    """Represent a lock based on a filesystem token.

    Takes ownership of a filesystem token.
    The caller is responsible for calling `.release()`
    exactly once to release the lock, removing the filesystem token.

    If the caller has not explicitly released the Lock before it is destroyed,
    a warning is issued and the __del__ method attempts to remove the object.
    """

    @property
    def name(self):
        """The name of the filesystem object provided when the lock was established."""
        return self._filesystem_object

    def __init__(self, filesystem_object=None):
        if not os.path.exists(filesystem_object):
            raise ValueError("Provided object is not a usable filesystem token.")
        self._filesystem_object = filesystem_object

    def is_active(self):
        return self._filesystem_object is not None

    def release(self):
        if self._filesystem_object is None:
            raise ValueError("Attempting to release an inactive lock.")
        _Lock._remove(self._filesystem_object)
        self._filesystem_object = None

    @staticmethod
    def _remove(path):
        """Remove a filesystem object"""
        if os.path.isfile(path):
            os.unlink(path)
        elif os.path.isdir(path):
            os.rmdir(path)
        else:
            warnings.warn("Cannot remove unknown filesystem object type: {}".format(path))

    def __del__(self):
        if self._filesystem_object is not None:
            warnings.warn(f"Lock object was not explicitly released! Token: {self._filesystem_object}")
            obj = self._filesystem_object
            self._filesystem_object = None
            if os.path.exists(obj):
                _Lock._remove(obj)


@contextlib.contextmanager
def scoped_directory_lock(path: typing.Union[str, bytes, os.PathLike] = None):
    """Create a lock directory at the provided path or in the current directory.

    Allows the caller to assert scoped control of a path through a filesystem
    token instead of inter/intra-process communication.

    Args:
        path: an existing filesystem path.

    Raises:
         LockException if the lock directory could not be created.

    Note:
        Historically, lock directories can be more effective than lock files on shared
        filesystems because of the atomicity of underlying filesystem commands. Feedback
        welcome.

    Caveats:
        * No check is currently performed on subdirectories or parents of the path
          to be locked.
        * The directory created as a lock token is assumed to remain empty.
    """
    if path is None:
        path = os.getcwd()
    token_path = _lock_directory(path)
    try:
        lock = _Lock(token_path)
    except ValueError as e:
        raise LockException("Could not create lock object.") from e
    try:
        yield lock
        # Control returns to the last line of the `try` block if the the `with`
        # block raises an exception.
    finally:
        # Note that unlink will fail if lock directory is not empty.
        # This may be a bug or a feature. Time will tell. But we should be very
        # careful about automatically removing non-empty directories on behalf
        # of users.
        if not lock.is_active():
            warnings.warn(f"Lock object {lock.name} does not exist!")
        else:
            lock.release()
