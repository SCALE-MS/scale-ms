"""Check that the basic Context implementation manages state as expected."""

import json
import os

import pytest
import scalems.context as _context
import scalems.context._datastore
import scalems.context._lock


def test_normal_lifecycle(tmp_path):
    """Test normal Context data store life cycle / state machine.

    get_context() should succeed when called in a clean directory or when
    the current process already owns the data store.
    """
    with _context.scoped_chdir(tmp_path):
        scalems.context._datastore.get_context()
        scalems.context._datastore.finalize_context()

        scalems.context._datastore.get_context()
        # Multiple calls to get_context() currently succeed as long as they are from
        # the process ID and not concurrent.
        scalems.context._datastore.get_context()

        scalems.context._datastore.finalize_context()
        # finalize_context() must be called exactly once for a data store that has been
        # opened.
        with pytest.raises(scalems.context._datastore.StaleFileStore):
            scalems.context._datastore.finalize_context()


def test_nonfinalized(tmp_path):
    """Failure to call finalize_context() should have well-defined behavior."""
    with _context.scoped_chdir(tmp_path):
        scalems.context._datastore.get_context()
        # In the future, more sophisticated tests might check that Singleton behavior or scoping
        # protections aren't violated.
        # Right now, we have to manipulate the filesystem with knowledge of the implementation
        # details in order to effectively test.
        with open(scalems.context._datastore._context_metadata_file, 'w') as fp:
            json.dump({'instance': 0}, fp)
        with pytest.raises(scalems.context._datastore.ContextError):
            scalems.context._datastore.get_context()
        # We may want to assert constraints on the filesystem state we expect to encounter
        # even when a lock is left unexpectedly, but initially all we know is that a
        # dangling lock may result from an unclean process termination.
        scalems.context._lock._lock_directory()
        with pytest.raises(scalems.context._datastore.ContextError):
            scalems.context._datastore.get_context()
        scalems.context._lock._unlock_directory()


def test_contention(tmp_path):
    """If two processes try to use the same data store, we should be able to
    detect and prevent it."""
    with _context.scoped_chdir(tmp_path):
        scalems.context._datastore.get_context()
        scalems.context._lock._lock_directory()
        with pytest.raises(scalems.context._lock.LockException):
            scalems.context._datastore.finalize_context()
        scalems.context._lock._unlock_directory()
        scalems.context._datastore.finalize_context()

        expected_instance = os.getpid()
        unexpected_instance = expected_instance + 1
        with open(scalems.context._datastore._context_metadata_file, 'w') as fp:
            json.dump({'instance': unexpected_instance}, fp)
        with pytest.raises(scalems.context._datastore.ContextError):
            scalems.context._datastore.get_context()

        scalems.context._lock._lock_directory()
        with pytest.raises(scalems.context._datastore.ContextError):
            scalems.context._datastore.get_context()
        scalems.context._lock._unlock_directory()
