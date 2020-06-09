# Check that the basic Context implementation manages state as expected.

import contextlib
import json
import os

import pytest
import scalems.context as _context


@contextlib.contextmanager
def working_directory(path):
    original_dir = os.getcwd()
    try:
        yield os.chdir(path)
    finally:
        if os.getcwd() != original_dir:
            os.chdir(original_dir)


def test_normal_lifecycle(tmp_path):
    """Test normal Context data store life cycle / state machine.

    get_context() should succeed when called in a clean directory or when
    the current process already owns the data store.
    """
    with working_directory(tmp_path):
        _context.get_context()
        _context.finalize_context()
        _context.get_context()
        _context.get_context()
        _context.finalize_context()
        with pytest.raises(_context.StaleFileStore):
            _context.finalize_context()


def test_nonfinalized(tmp_path):
    """Failure to call finalize_context() should have well-defined behavior."""
    with working_directory(tmp_path):
        _context.get_context()
        # In the future, more sophisticated tests might check that Singleton behavior or scoping
        # protections aren't violated.
        # Right now, we have to manipulate the filesystem with knowledge of the implementation
        # details in order to effectively test.
        with open(_context._context_metadata_file, 'w') as fp:
            json.dump({'instance': 0}, fp)
        with pytest.raises(_context.ContextError):
            _context.get_context()
        # We may want to assert constraints on the filesystem state we expect to encounter
        # even when a lock is left unexpectedly, but initially all we know is that a
        # dangling lock may result from an unclean process termination.
        _context._lock_directory()
        with pytest.raises(_context.ContextError):
            _context.get_context()
        _context._unlock_directory()


def test_contention(tmp_path):
    """If two processes try to use the same data store, we should be able to
    detect and prevent it."""
    with working_directory(tmp_path):
        _context.get_context()
        _context._lock_directory()
        with pytest.raises(_context.LockException):
            _context.finalize_context()
        _context._unlock_directory()
        _context.finalize_context()

        expected_instance = os.getpid()
        unexpected_instance = expected_instance + 1
        with open(_context._context_metadata_file, 'w') as fp:
            json.dump({'instance': unexpected_instance}, fp)
        with pytest.raises(_context.ContextError):
            _context.get_context()

        _context._lock_directory()
        with pytest.raises(_context.ContextError):
            _context.get_context()
        _context._unlock_directory()
