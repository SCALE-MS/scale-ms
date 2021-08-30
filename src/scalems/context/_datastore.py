"""Manage non-volatile data for SCALE-MS in a filesystem context."""

__all__ = [
    'ContextError',
    'StaleFileStore',
    'finalize_context',
    'get_context',
]

import json
import logging
import os

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
