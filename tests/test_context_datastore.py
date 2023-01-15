"""Check that the basic Context implementation manages state as expected."""

import json
import logging
import os
import tempfile

import pytest
import scalems.context as _context
import scalems.exceptions
from scalems.store import filestore_generator
from scalems.store import FileStore, FileStoreManager


def test_normal_lifecycle(tmp_path, caplog):
    """Test normal Context data store life cycle / state machine.

    initialize_datastore() should succeed when called in a clean directory or when
    the current process already owns the data store.
    """
    datastore = FileStore(directory=tmp_path)
    assert not datastore.closed
    assert datastore.instance == os.getpid()
    with open(datastore.filepath, "r") as fh:
        assert json.load(fh)["instance"] == os.getpid()
    datastore.close()
    assert datastore.closed
    with open(datastore.filepath, "r") as fh:
        data = json.load(fh)
    assert "instance" in data
    assert data["instance"] is None

    generator = filestore_generator(directory=tmp_path)
    datastore = next(generator)()
    # Disallow multiple FileStores per directory.
    with pytest.raises(scalems.exceptions.ContextError):
        assert tmp_path in FileStore._instances
        assert FileStore._instances[tmp_path] is datastore
        FileStore(directory=tmp_path)
    datastore.close()
    # finalize_datastore() must be called exactly once for a data store that has been
    # opened.
    with pytest.raises(scalems.store.StaleFileStore):
        datastore.close()
    with pytest.raises(StopIteration):
        with pytest.warns(scalems.exceptions.ScaleMSWarning):
            next(generator)
    del generator

    # Confirm some assumptions about implementation details.
    filepath = tmp_path.joinpath(scalems.store._data_subdirectory, scalems.store._metadata_filename)

    manager = FileStoreManager(tmp_path)
    datastore: scalems.store.FileStore = manager.filestore()
    assert datastore.instance == os.getpid()
    assert datastore.filepath == filepath
    with pytest.raises(AttributeError):
        # `log` attribute is not assignable.
        datastore.log = list()
    # TODO: log interface.
    # datastore.log.append('Testing')
    with caplog.at_level(logging.CRITICAL, logger="scalems.store"):
        assert not datastore.closed
        del manager
        assert datastore.closed
    with open(filepath, "r") as fh:
        metadata: dict = json.load(fh)
        # A finalized record should not have an owning *instance*.
        assert metadata["instance"] is None


def test_nonfinalized(tmp_path, caplog):
    """Failure to call finalize_datastore() should have well-defined behavior."""
    with _context.scoped_chdir(tmp_path):
        manager = FileStoreManager()
        datastore = manager.filestore()
        metadata_path = datastore.filepath

        # Fake a bad shutdown.
        with open(metadata_path, "w") as fp:
            json.dump({"instance": 0}, fp)
        # In the future, more sophisticated tests might check that Singleton behavior or scoping
        # protections aren't violated.
        # Right now, we have to manipulate the filesystem with knowledge of the implementation
        # details in order to effectively test.
        with pytest.raises(scalems.exceptions.ContextError):
            FileStoreManager().filestore()
        # We may want to assert constraints on the filesystem state we expect to encounter
        # even when a lock is left unexpectedly, but initially all we know is that a
        # dangling lock may result from an unclean process termination.
        scalems.store._lock._lock_directory()
        with pytest.raises(scalems.exceptions.ContextError):
            FileStoreManager()
        scalems.store._lock._unlock_directory()

        logger = logging.getLogger("scalems.context._datastore")
        logger.debug(
            "Suppressing an error that would normally occur when we fail to "
            "close the FileStore (that we intentionally broke in this test)."
        )
        with caplog.at_level(logging.CRITICAL, logger="scalems.store"):
            del manager


def test_contention(tmp_path, caplog):
    """Test various ways data stores could collide.

    If two processes try to use the same data store, we should be able to
    detect and prevent it.
    """
    with _context.scoped_chdir(tmp_path):
        manager = FileStoreManager()
        datastore = manager.filestore()
        scalems.store._lock._lock_directory()
        with pytest.raises(scalems.store._lock.LockException):
            datastore.close()
        # This failed call to `close` leaves the datastore in an invalid state.
        with pytest.raises(scalems.exceptions.ContextError):
            assert not datastore.closed

        scalems.store._lock._unlock_directory()
        with caplog.at_level(logging.CRITICAL, logger="scalems.store"):
            # The above failed `close` left the manager in an invalid state.
            del manager

        metadata_path = datastore.filepath
        expected_instance = os.getpid()
        unexpected_instance = expected_instance + 1
        with open(metadata_path, "w") as fp:
            json.dump({"instance": unexpected_instance}, fp)
        with pytest.raises(scalems.exceptions.ContextError):
            FileStoreManager()

        scalems.store._lock._lock_directory()
        with pytest.raises(scalems.exceptions.ContextError):
            FileStoreManager()
        scalems.store._lock._unlock_directory()


def test_nesting(tmp_path):
    """Test various ways data stores could collide.

    We should check for old scalems data store versions or broken/invalid directory
    structures.
    """
    # initialization should check for locks in parent directories.
    with scalems.store._lock.scoped_directory_lock(tmp_path):
        with tempfile.TemporaryDirectory(dir=tmp_path) as path:
            with _context.scoped_chdir(path):
                with pytest.raises(scalems.exceptions.ContextError):
                    FileStoreManager(directory=path)

    # initialization should check for unexpected nesting in the work dir.
    manager = FileStoreManager(directory=tmp_path)
    datastore = manager.filestore()
    root_path = datastore.directory
    assert root_path == tmp_path
    datastore_path = datastore.datastore
    with tempfile.TemporaryDirectory(dir=root_path) as path:
        with _context.scoped_chdir(path):
            with pytest.raises(scalems.exceptions.ContextError):
                FileStoreManager()

    # initialization should check for unexpected nesting in the datastore itself.
    with _context.scoped_chdir(datastore_path):
        with pytest.raises(scalems.exceptions.ContextError):
            FileStoreManager()


def test_recovery(tmp_path):
    """A workflow directory should be re-usable if it was shut down cleanly."""

    # Follow the lifecycle of a workflow session.
    manager = FileStoreManager(directory=tmp_path)
    datastore = manager.filestore()
    metadata_path = datastore.filepath
    del manager

    with open(metadata_path, "r") as fh:
        metadata: dict = json.load(fh)
        assert metadata["instance"] is None

    manager = FileStoreManager(directory=tmp_path)
    assert datastore is not manager.filestore()
    assert datastore.closed
    assert not manager.filestore().closed
    with open(metadata_path, "r") as fh:
        metadata: dict = json.load(fh)
        assert metadata["instance"] == os.getpid()
