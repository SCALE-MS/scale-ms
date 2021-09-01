"""Manage non-volatile data for SCALE-MS in a filesystem context.

The data store must be active before attempting any manipulation of the managed workflow.
The data store must be closed by the time control returns to the interpreter after
leaving a managed workflow scope (and releasing a WorkflowManager instance).

We prefer to do this with the Python context manager protocol, since we already rely on
`scalems.workflow.scope()`.

We can also use a contextvars.ContextVar to hold a weakref to the FileStore, and register
a finalizer to perform a check, but the check could come late in the interpreter shutdown
and we should not rely on it. Also, note the sequence with which module variables and
class definitions are released during shutdown.

TODO: Make FileStore look more like a File interface, with open and close and context
    manager support, and provide boolean status properties. Let get_context() return the
    currently in-scope FileStore.
"""

__all__ = [
    'ContextError',
    'StaleFileStore',
    'finalize_context',
    'get_context',
]

import dataclasses
import json
import logging
import os
import pathlib
import typing
import warnings

import scalems.exceptions
from ._lock import LockException
from ._lock import scoped_directory_lock as _scoped_directory_lock

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

_context_metadata_file = '.scalems_context_metadata.json'
"""Name to use for Context metadata files (module constant)."""


class StaleFileStore(Exception):
    """The backing file store is not consistent with the known (local) history."""


class ContextError(Exception):
    """A Context operation could not be performed."""


@dataclasses.dataclass
class Metadata:
    instance: int


class FileStore:
    """Handle to the SCALE-MS nonvolatile data store for a workflow context.

    Not thread safe. User is responsible for serializing access, as necessary.

    Fields:
        instance (int): Owner's PID.
        log (list): Access log for the data store.
        path (pathlib.Path): filesystem path to metadata JSON file.

    """
    _data: Metadata
    _fields: typing.ClassVar = set([field.name for field in dataclasses.fields(Metadata)])
    log: typing.Sequence[str]
    path: pathlib.Path

    def __setattr__(self, key, value):
        # Note: called for all attribute assignments.
        if key in self._fields:
            setattr(self._data, key, value)
        else:
            raise AttributeError(
                f'Cannot assign to field {key}'
            )

    def __getattr__(self, item):
        # Note: only called when default attribute access fails.
        if item not in FileStore._fields:
            raise AttributeError(f'No field called {item}.')
        else:
            return getattr(self._data, item)

    def __init__(self, dir: pathlib.Path):
        # TODO: Consider breaking up the complex initialization with multi-state
        #  FileStore and separate creation function.
        metadata_file = (pathlib.Path(dir) / _context_metadata_file).absolute()
        self.__dict__['path'] = metadata_file

        # TODO: Use a log file!
        self.__dict__['log'] = []

        instance_id = os.getpid()
        try:
            with _scoped_directory_lock(dir):
                logger.debug(f'Initializing {metadata_file}.')
                if metadata_file.exists():
                    logger.debug('Restoring metadata from previous metadata file.')
                    with open(metadata_file, 'r') as fp:
                        metadata_dict = json.load(fp)
                    if 'instance' in metadata_dict:
                        # The metadata file has an owner.
                        if metadata_dict['instance'] != instance_id:
                            # This would be a good place to check a heart beat or otherwise
                            # try to confirm whether the previous owner is still active.
                            raise ContextError('Context is already in use.')
                        else:
                            assert metadata_dict['instance'] == instance_id
                            w = scalems.exceptions.ProtocolWarning(
                                'Inappropriate `get_context()` call. This process is '
                                f'already managing {metadata_file}'
                            )
                            warnings.warn(w)
                    else:
                        # The metadata file has no owner
                        logger.debug(
                            f'Taking ownership of metadata file for PID {instance_id}')
                        metadata_dict['instance'] = instance_id
                else:  # metadata_file does not yet exist.
                    logger.debug(
                        f'Creating metadata file for PID {instance_id}')
                    metadata_dict = {'instance': instance_id}
                self.__dict__['_data'] = Metadata(**metadata_dict)
                with open(metadata_file, 'w') as fp:
                    json.dump(dataclasses.asdict(self._data), fp)
        except LockException as e:
            raise ContextError(
                'Could not acquire ownership of working directory {}'.format(dir)) from e


def get_context() -> FileStore:
    """Get a reference to an API Context instance.

    If get_context() succeeds, the caller is responsible for calling `finalize_context()`
    before the interpreter exits.

    Raises:
        ContextError if context backing store could not be obtained.
    """
    path = pathlib.Path(os.getcwd())
    return FileStore(dir=path)


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
