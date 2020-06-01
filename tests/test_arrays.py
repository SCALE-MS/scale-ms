import hashlib
import unittest

from serialization import Integer64


# Question: where can/should we optimize data store and checkpoint writers to
#  reduce data transformations? We need to be able to write blocks of serialized
#  data, but we should allow optimizations for known data formats.

class DataStore:
    """Abstract interface for stateful data resources."""
    class ReadHandle:
        """DataStore handle open for read-only access."""

    class WriteHandle:
        """DataStore handle open for writing."""

    def open(self, mode):
        # Check file state.
        # If safe, open file and return handle.
        pass

    def close(self):
        # Close and release filehandle, but retain access to the backing resources
        # and maximize the ability to reopen the file through a later call.
        pass


class CheckpointPublisher:
    """Update checkpoint data store with published state.

    Provide a standard publishing interface for arbitrary backing store.

    Instances are composed for the state data structures of distinct operations.
    """


class CheckpointFacility:
    """Manage file-backed checkpoint state for a concrete node.
    """
    def __init__(self, context, node):
        self.uid = get_uid(node)
        self.datastore = get_datastore(context, node)

    def write(self, state):
        """Update the checkpoint with new state."""

    def load(self, receiver):
        """Apply the checkpoint state to receiver.

        This aspect will evolve to a Director that can be applied to an
        operation node builder.
        """


class WorkerContext:
    """Simple data staging context with file-backed checkpoints."""
    def __init__(self, working_directory='.'):
        self.storage_system = os.path.abspath(working_directory)
        # TODO: On initialization, we should check for a clean and usable working directory.
        # TODO: We can also preemptively discover existing checkpoint state.


def get_filesystem(context):
    """Get the root filesystem node managed by the Context."""
    return context.storage_system


def get_store_type(context, node):
    # In a given Context, we could choose a file container type based on data
    # type and/or local configuration, such as JSON, pickle, numpy, HDF5, XDR, etc.
    return 'numpy'


def node_uid(node):
    pass


def lock_store(store, owner):
    """Get data store handle for owner.

    Assert ownership of the data store. If the store has no declared owner,
    take ownership. If the store has a declared owner, confirm the ownership
    matches.

    Raises:
        OwnershipError: if ownership cannot be established.
        ValueError: if *store* or *owner* are invalid.

    Returns:
        DataStore handle, with ownership established.

    Warning:
        It is the responsibility of the calling framework to release ownership
        of the data store, even in the event of exceptional termination. We will
        not bother to make this more robust, because the locking will move to
        the level of the Context resources as part of the Session context manager.
    """



def get_datastore(context, node):
    """Get the storage handle for a node in this context.

    If there is not yet a storage resource for the node, initialize one.
    """
    # Design roadmap
    # 1. Initialize or discover valid data store.
    #    * create initialized data store in temporary directory with ownership claim.
    #    * try to move to canonical location.
    #    * if canonical location already exists, reinitialize from existing store
    #      and remove temporary data store.
    # 2. Take ownership of valid data store.
    #    * if canonical location already exists, attempt to lock (create lock directory with ownership metadata)
    #    * if lock already exists, produce error output. else, reinitialize from store.
    # 3. Remove lock information when WorkerContext releases DataStore handle.

    import json
    import numpy
    import tempfile

    storage_system = get_filesystem(context)

    uid = node_uid(node)

    store_type = get_store_type(context, node)

    # 1. Create an initialized data store in a temporary directory.
    # Create an initialized data store in a temporary directory.
    temp_store = tempfile.mkdtemp(dir=os.path.join(storage_system, 'tmp'))
    # describe data store type, fingerprinting details...
    metadata = {'uid': uid,
                'file_type': store_type}
    with open(os.path.join(temp_store, 'metadata.json'), 'w') as fh:
        json.dump(fh, metadata)
    # "lock" the data store for the current context.
    owner = {'pid': os.getpid(),
              'object': id(context)}
    lockfile = os.path.join(temp_store, 'owner')
    assert not os.path.exists(lockfile)
    with open(lockfile, 'w') as fh:
        json.dump(fh, owner)
    # Create artifact
    npz_file = os.path.join(temp_store, '.'.join([uid, 'npz']))
    data = numpy.zeros(shape=node_shape(node), dtype=node_dtype(node))
    numpy.savez(npz_file, data=data)


    # 2. Try to rename the directory to the canonical name.
    #    In order to get an exception, os.rename destination needs to clash with
    #    a non-empty directory.
    store_path = os.path.join(storage_system, uid)
    try:
        os.rename(temp_store, store_path)
        store = lock_store(store_path, owner)
    except OSError as e:
        # Ref https://docs.python.org/3/library/os.html#os.rename
        # for more specific exceptions.

        # 3. If the canonical directory name is already in use, try to take ownership.
        try:
            # 4. If ownership taken, reinitialize from its contents.
            store = lock_store(store_path, owner)
        except:
            # TODO: What are the exception conditions?
            raise

    # TODO: Protocol for unlocking the backing store OR lock Context instead.
    return store


def test_task_details():
    """Mock up the Context node builder protocol to test internal details."""
    my_array = Integer64([[1, 2], [3, 4]])

    # Receive record of node
    record = my_array.to_json()

    # Create a local handle to the node
    # 1. Check whether the node already exists.
    # 2. Initialize checkpoint facility for the node.
    # 3. Allow checkpoint facility to update the node.
    # 4. Subscribe to needed resources.



    # Accept queries to the node status

    # Accept subscriptions to the node

    # Accept a request to publish the node results (trigger execution)

class Integer64TestCase(unittest.TestCase):
    def test_uid(self):
        # Test fingerprinting for Integer64
        my_array = Integer64([[1, 2], [3, 4]])
        expected_json = '{"depends":[],"input":{"data":[[1,2],[3,4]]},"operation":["scalems","Integer64"]}'

        fingerprint = my_array.fingerprint()

        actual_json = fingerprint.compact_json()
        assert expected_json == actual_json

        expected_hash = hashlib.sha256(expected_json.encode('utf-8')).digest()
        actual_hash = my_array.fingerprint().uid()
        assert expected_hash == actual_hash

    def test_serialization(self):
        data = [[1, 2], [3, 4]]
        my_array = Integer64(data)
        expected_uid = my_array.fingerprint().uid()  # type: bytes
        record = my_array.to_json()
        my_array = Integer64.from_json(record)
        assert my_array.fingerprint().uid() == expected_uid
        assert (my_array.data == data).all()
