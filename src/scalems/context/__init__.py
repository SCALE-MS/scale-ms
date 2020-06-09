"""Execution environment context management.
"""

__all__ = ['scoped_chdir']

import contextlib
import json
import logging
import os
import pathlib
import threading
import typing
import warnings

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


cwd_lock = threading.Lock()


@contextlib.contextmanager
def scoped_chdir(directory: typing.Union[str, bytes, os.PathLike]):
    """Restore original working directory when exiting the context manager.

    Caveats:
        Current working directory is a process-level property. To avoid unexpected
        behavior across threads, only one instance of this context manager may be
        active at a time. If necessary, we could allow for nested cwd contexts,
        but we cannot make this behavior thread-safe.

    """
    if isinstance(directory, bytes):
        directory = os.fsdecode(directory)
    path = pathlib.Path(directory)
    if not path.exists() or not path.is_dir():
        raise ValueError(f'Not a valid directory: {str(directory)}')
    if cwd_lock.locked():
        warnings.warn('Another call has already used scoped_chdir. Waiting for lock...')
    with cwd_lock:
        oldpath = os.getcwd()
        os.chdir(path)
        logger.debug(f'Changed current working directory to {path}')
        try:
            yield path
            # If the `with` block using scoped_chdir produces an exception, it will
            # be raised at this point in this function. We want the exception to
            # propagate out of the `with` block, but first we want to restore the
            # original working directory, so we skip `except` but provide a `finally`.
        finally:
            logger.debug(f'Changing working directory back to {oldpath}')
            os.chdir(oldpath)
    logger.info('scoped_chdir exited.')


_lock_directory_name = '.scalems_lock'
"""Name to use for lock directories (module constant)"""


_context_metadata_file = '.scalems_context_metadata.json'
"""Name to use for Context metadata files (module constant)."""


class LockException(Exception):
    """The requested lock could not be obtained."""


class StaleFileStore(Exception):
    """The backing file store is not consistent with the known (local) history."""


class ContextError(Exception):
    """A Context operation could not be performed."""


def get_context():
    """Get a reference to an API Context instance.

    If get_context() succeeds, the caller is responsible for calling `finalize_context()`
    before the interpreter exits.

    Raises:
        ContextError if context backing store could not be obtained.
    """
    path = os.getcwd()
    instance_id = os.getpid()
    try:
        context = {}
        with _scoped_directory_lock(path):
            if os.path.exists(_context_metadata_file):
                with open(_context_metadata_file, 'r') as fp:
                    context.update(json.load(fp))
            if 'instance' in context:
                if context['instance'] != instance_id:
                    # This would be a good place to check a heart beat or otherwise
                    # try to confirm whether the previous owner is still active.
                    raise ContextError('Context is already in use.')
                else:
                    assert context['instance'] == instance_id
            else:
                context['instance'] = instance_id
            with open(_context_metadata_file, 'w') as fp:
                json.dump(context, fp)
        return context
    except LockException as e:
        raise ContextError('Could not acquire ownership of working directory {}'.format(path)) from e


def finalize_context():
    """Mark the local Context data store as inactive.

    Finalize the state of the file-backed Context. Allow future get_context()
    calls to succeed whether coming from this process or another.

    Must be called at some point after a get_context() call succeeds in order
    to clean up local state and allow other processes to use the data store.
    """
    # A Python `with` block (the context manager protocol) is the most appropriate way to enforce
    # scoped clean-up, but there may be other reasons (such as user-friendly procedural interface
    # support) to try to use object lifetime or even interpreter process lifetime to try to
    # finalize the Context metadata. A reasonable pattern might be to hold a "state machine"
    # object, implemented as a generator. Attached (with a weakref) to the Context, it could
    # be made responsible for advancing (updating) the context state, with a `finally` block
    # to finalize the Context when the state machine reaches an end condition or the generator
    # is otherwise finalized. See https://docs.python.org/3.6/reference/expressions.html#yield-expressions
    # Note also that an object can probably be both a generator and a context manager.
    # The standard library decorator helpers for these behaviors are not compatible, but such a
    # duality might come in handy if we need to migrate from one protocol to the other or even support both.
    path = os.getcwd()
    metadata_filename = os.path.join(path, _context_metadata_file)
    try:
        file_context = open(metadata_filename, 'r')
    except OSError as e:
        raise StaleFileStore('Could not open metadata file.') from e
    with file_context as fp:
        current_instance = os.getpid()
        context = json.load(fp)
        stored_instance = None
        if 'instance' in context:
            stored_instance = context['instance']
        if stored_instance != current_instance:
            # Note that the StaleFileStore check will be more intricate as metadata becomes more sophisticated.
            raise StaleFileStore('Expected ownership by {}, but found {}'.format(current_instance, stored_instance))
    with _scoped_directory_lock(path):
        del context['instance']
        with open(metadata_filename, 'w') as fp:
            json.dump(context, fp)


def _lock_directory(path=None):
    """Provide a fall-back mechanism for locking a directory.

    Locks must be explicitly removed by calling _unlock_directory(path)

    Raises:
        LockException if the lock could not be obtained.

    """
    if path is None:
        path = os.getcwd()
    token_path = os.path.join(path, _lock_directory_name)
    try:
        os.mkdir(token_path)
    except FileExistsError as e:
        # Directory lock already exists.
        raise LockException('{} already exists'.format(token_path)) from e
    return token_path


def _unlock_directory(path=None):
    """Remove a lock from a directory path.

    This must be called to release a lock established with _lock_directory(path).
    """
    if path is None:
        path = os.getcwd()
    token_path = os.path.join(path, _lock_directory_name)
    assert os.path.exists(token_path)
    os.rmdir(token_path)


class _Lock:
    """Represent a lock based on a filesystem token.

    Takes ownership of a filesystem token.
    The caller is responsible for calling `.release()`
    exactly once to release the lock, removing the filesystem token.

    If the caller has not explicitly released the Lock before it is destroyed,
    a warning is issued and the __del__ method attempts to remove the object.
    """
    def __init__(self, filesystem_object=None):
        if not os.path.exists(filesystem_object):
            raise ValueError('Provided object is not a usable filesystem token.')
        self._filesystem_object = filesystem_object

    def is_active(self):
        return self._filesystem_object is not None

    def release(self):
        if self._filesystem_object is None:
            raise ValueError('Attempting to release an inactive lock.')
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
            warnings.warn('Cannot remove unknown filesystem object type: {}'.format(path))

    def __del__(self):
        if self._filesystem_object is not None:
            warnings.warn('Lock object was not explicitly released! Token: {}'.format(self._filesystem_object))
            obj = self._filesystem_object
            self._filesystem_object = None
            if os.path.exists(obj):
                _Lock._remove(obj)


@contextlib.contextmanager
def _scoped_directory_lock(path: typing.Union[str, bytes, os.PathLike] = None):
    """Create a lock directory at the provided path or in the current directory.

    Allows the caller to assert scoped control of a path through a filesystem
    token instead of inter/intra-process communication.
    Lock directories can be more effective than lock files on shared filesystems
    because of the atomicity of underlying filesystem commands.

    Note that *path* must already exist.

    Raises LockException if the lock directory could not be created.

    Caveats:
        * No check is currently performed on subdirectories or parents of the path to be locked.
        * The directory created as a lock token is assumed to remain empty.
    """
    # The token name is effectively a module constant.
    if path is None:
        path = os.getcwd()
    token_path = _lock_directory(path)
    try:
        lock = _Lock(token_path)
    except ValueError as e:
        raise LockException('Could not create lock object.') from e
    try:
        yield lock
        # Control returns to the last line of the `try` block if the the `with`
        # block raises an exception.
    finally:
        # Note that unlink will fail if lock directory is not empty.
        # This may be a bug or a feature. Time will tell. But we should be very
        # careful about automatically removing non-empty directories on behalf
        # of users.
        if lock.is_active():
            lock.release()
