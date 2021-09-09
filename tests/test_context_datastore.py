"""Check that the basic Context implementation manages state as expected."""

import json
import os

import pytest
import scalems.context as _context
import scalems.context._datastore
import scalems.context._lock
import scalems.exceptions


def test_normal_lifecycle(tmp_path):
    """Test normal Context data store life cycle / state machine.

    initialize_datastore() should succeed when called in a clean directory or when
    the current process already owns the data store.
    """
    with _context.scoped_chdir(tmp_path):
        datastore = scalems.context._datastore.initialize_datastore()
        assert not datastore.closed
        assert datastore.instance == os.getpid()
        with open(datastore.filepath, 'r') as fh:
            assert json.load(fh)['instance'] == os.getpid()
        datastore.close()
        assert datastore.closed
        with open(datastore.filepath, 'r') as fh:
            data = json.load(fh)
        assert 'instance' in data
        assert data['instance'] is None

        datastore = scalems.context._datastore.initialize_datastore()
        # Multiple calls to initialize_datastore() currently succeed as long as they are from
        # the process ID and not concurrent.
        with pytest.raises(scalems.context.ContextError):
            scalems.context._datastore.initialize_datastore()

        datastore.close()
        # finalize_datastore() must be called exactly once for a data store that has been
        # opened.
        with pytest.raises(scalems.context.StaleFileStore):
            datastore.close()

    # Confirm some assumptions about implementation details.
    filepath = tmp_path.joinpath(scalems.context._datastore._data_subdirectory,
                                 scalems.context._datastore._metadata_filename)

    with _context.scoped_chdir(tmp_path):
        datastore = scalems.context._datastore.initialize_datastore()
        assert datastore.instance == os.getpid()
        assert datastore.filepath == filepath
        with pytest.raises(AttributeError):
            datastore.log = list()
        # TODO: log interface.
        # datastore.log.append('Testing')
        datastore.close()
    with open(filepath, 'r') as fh:
        metadata: dict = json.load(fh)
        # A finalized record should not have an owning *instance*.
        assert metadata['instance'] is None


def test_nonfinalized(tmp_path):
    """Failure to call finalize_datastore() should have well-defined behavior."""
    with _context.scoped_chdir(tmp_path):
        with scalems.context._datastore.initialize_datastore() as datastore:
            metadata_path = datastore.filepath
        # Fake a bad shutdown.
        with open(metadata_path, 'w') as fp:
            json.dump({'instance': 0}, fp)
        # In the future, more sophisticated tests might check that Singleton behavior or scoping
        # protections aren't violated.
        # Right now, we have to manipulate the filesystem with knowledge of the implementation
        # details in order to effectively test.
        with pytest.raises(scalems.context.ContextError):
            datastore = scalems.context._datastore.initialize_datastore()
        # We may want to assert constraints on the filesystem state we expect to encounter
        # even when a lock is left unexpectedly, but initially all we know is that a
        # dangling lock may result from an unclean process termination.
        scalems.context._lock._lock_directory()
        with pytest.raises(scalems.context.ContextError):
            scalems.context._datastore.initialize_datastore()
        scalems.context._lock._unlock_directory()


def test_contention(tmp_path):
    """If two processes try to use the same data store, we should be able to
    detect and prevent it."""
    with _context.scoped_chdir(tmp_path):
        datastore = scalems.context._datastore.initialize_datastore()
        scalems.context._lock._lock_directory()
        with pytest.raises(scalems.context._lock.LockException):
            datastore.close()
        scalems.context._lock._unlock_directory()
        datastore.close()

        metadata_path = datastore.filepath
        expected_instance = os.getpid()
        unexpected_instance = expected_instance + 1
        with open(metadata_path, 'w') as fp:
            json.dump({'instance': unexpected_instance}, fp)
        with pytest.raises(scalems.context.ContextError):
            scalems.context._datastore.initialize_datastore()

        scalems.context._lock._lock_directory()
        with pytest.raises(scalems.context._datastore.ContextError):
            scalems.context._datastore.initialize_datastore()
        scalems.context._lock._unlock_directory()


def test_recovery(tmp_path):
    """A workflow directory should be re-usable if it was shut down cleanly."""

    # Follow the lifecycle of a workflow session.
    with _context.scoped_chdir(tmp_path):
        with scalems.context.initialize_datastore() as datastore:
            metadata_path = datastore.filepath
        with open(metadata_path,
                  'r') as fh:
            metadata: dict = json.load(fh)
            assert metadata['instance'] is None

        with scalems.context.initialize_datastore() as datastore:
            with open(metadata_path,
                      'r') as fh:
                metadata: dict = json.load(fh)
                assert metadata['instance'] == os.getpid()
