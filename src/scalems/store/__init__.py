"""Manage non-volatile data for SCALE-MS in a filesystem context.

Implement several interfaces from `scalems.file`.

The data store must be active before attempting any manipulation of the managed workflow.
The data store must be closed by the time control returns to the interpreter after
leaving a managed workflow scope (and releasing a WorkflowManager instance).

We prefer to do this with the Python context manager protocol, since we already rely on
`scalems.workflow.scope()`.

We can also use a contextvars.ContextVar to hold a weakref to the FileStore, and register
a finalizer to perform a check, but the check could come late in the interpreter shutdown
and we should not rely on it. Also, note the sequence with which module variables and
class definitions are released during shutdown.
"""

from __future__ import annotations

__all__ = (
    "StaleFileStore",
    "filestore_generator",
    "get_file_reference",
    "FileReference",
    "FileStore",
    "FileStoreManager",
    "FilesView",
    "Metadata",
)

import asyncio
import contextvars
import dataclasses
import functools
import json
import logging
import os
import pathlib
import shutil
import tempfile
import threading
import typing
import weakref

import scalems.exceptions as _exceptions
from . import _lock
from .. import file as _file
from .. import identifiers as _identifiers

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

_data_subdirectory = "scalems_0_0"
"""Subdirectory name to use for managed filestore."""

_metadata_filename = "scalems_context_metadata.json"
"""Name to use for Context metadata files (module constant)."""


class StaleFileStore(_exceptions.ContextError):
    """The backing file store is not consistent with the known (local) history."""


@dataclasses.dataclass
class Metadata:
    """Simple container for metadata at run time.

    This is intentionally decoupled from the non-volatile backing store or any usage
    protocols. Instances are managed through the FileStore.

    We will probably prefer to provide distinct interfaces for managing non-volatile
    assets (files) and for persistent workflow state (metadata), which may ultimately be
    supported by a single FileStore. That would likely involve replacing rather than
    extending this class, so let's keep it as a simple container for now:
    Make sure that this dataclass remains easily serialized and deserialized.

    For simplicity, this data structure should be easily serializable. Where necessary,
    mappings to and from non-trivial types must be documented.
    """

    instance: int
    """Unique identifier for this data store."""

    files: typing.MutableMapping[str, str] = dataclasses.field(default_factory=dict)
    """Managed filesystem objects.

    Maps hexadecimal-encoded (str) ResourceIdentifiers to filesystem paths (str).
    """

    objects: typing.MutableMapping[str, dict] = dataclasses.field(default_factory=dict)
    """Dictionary-encoded objects.

    Refer to `scalems.serialization` for encoding schema.
    """


class FilesView(typing.Mapping[_identifiers.ResourceIdentifier, pathlib.Path]):
    """Read-only viewer for files metadata.

    Provides a mapping from :py:class:`~scalems.identifiers.ResourceIdentifier` to
    the stored filesystem path.

    Warnings:
        The :py:class:`~pathlib.Path` objects represent paths in the context of
        the filesystem of the FileStore. If the FileStore does not represent a local
        filesystem, then a ``file://`` URIs from ``value.as_uri()`` is specific
        to the remote filesystem.
    """

    def __init__(self, files: typing.Mapping[str, str]):
        # Get a strong reference to the data structure provided by the filestore.
        self._files = files

    def __getitem__(self, k: _identifiers.ResourceIdentifier | str) -> pathlib.Path:
        return pathlib.Path(self._files[str(k)])

    def __contains__(self, k: _identifiers.ResourceIdentifier | str) -> bool:
        return str(k) in self._files

    def __len__(self) -> int:
        return len(self._files)

    def __iter__(self) -> typing.Iterator[_identifiers.ResourceIdentifier]:
        for key in self._files:
            yield _identifiers.ResourceIdentifier(bytes.fromhex(key))


class FileStore(typing.Mapping[_identifiers.ResourceIdentifier, "FileReference"]):
    """Handle to the SCALE-MS nonvolatile data store for a workflow context.

    Used as a Container, serves as a Mapping from identifiers to resource references.

    TODO: Consider generalizing the value type of the mapping for general Resource types, per the scalems data model.
    """

    _fields: typing.ClassVar = set([field.name for field in dataclasses.fields(Metadata)])
    _instances: typing.ClassVar = weakref.WeakValueDictionary()

    _token: contextvars.Token = None
    _data: Metadata
    _datastore: pathlib.Path = None
    _directory: pathlib.Path = None
    _filepath: pathlib.Path = None
    _update_lock: threading.Lock
    _dirty: threading.Event
    _log: typing.Sequence[str]
    _repr: str = "<FileStore>"

    @property
    def _tmpfile_prefix(self) -> str:
        """Filename generation helper for internal use."""
        return f"tmp_{self.instance}_"

    @property
    def log(self):
        """Get access to the operation log.

        Interface TBD...
        """
        for line in self._log:
            yield line

    # TODO: Consider a caching proxy to the directory structure to reduce filesystem
    #  calls.

    @property
    def directory(self) -> pathlib.Path:
        """The work directory under management.

        Generally, this is the current working directory when a scalems script is
        launched. The user is free to use the work directory to stage input and output
        files with the exception of the single scalems datastore
        directory. See `FileStore.datastore`.

        scalems will create a hidden "lock" directory briefly when starting up or
        shutting down. You may need to manually remove the lock directory after a
        particularly bad crash.
        """
        return self._directory

    @property
    def datastore(self) -> pathlib.Path:
        """Path to the data store for the workflow managed at *directory*.

        scalems creates a subdirectory at the root of a workflow in which to manage
        internal data. Editing files or directories in this subdirectory will affect
        workflow state and validity.

        The name reflects the SCALE-MS data format version and is not user-configurable.
        """
        if self._datastore is None:
            self._datastore = self.directory / _data_subdirectory
        return self._datastore

    @property
    def filepath(self) -> pathlib.Path:
        """Path to the metadata backing store.

        This is the metadata file used by scalems to track workflow state and
        file-backed data. Its format is closely tied to the scalems API level.
        """
        if self._filepath is None:
            self._filepath = self.datastore / _metadata_filename
        return self._filepath

    @property
    def instance(self):
        return self._data.instance

    def __repr__(self):
        return str(self._repr)

    def __getitem__(self, __k: _identifiers.ResourceIdentifier | str) -> "FileReference":
        return FileReference(filestore=self, key=__k)

    def __len__(self) -> int:
        return len(self._data.files)

    def __iter__(self) -> typing.Iterator[_identifiers.ResourceIdentifier]:
        return iter(self.files)

    def __init__(self, *, directory: pathlib.Path):
        """Assemble the data structure.

        Users should not create FileStore objects directly, but with
        initialize_datastore() or through the WorkflowManager instance.

        Once initialized, caller is responsible for calling the close() method.
        The easiest way to do this is to avoid creating the FileStore directly,
        and instead use a FileStoreManager object.

        No directory in the filesystem should be managed by more than one FileStore.
        The FileStore class maintains a registry of instances to prevent instantiation of
        a new FileStore for a directory that is already managed.

        Raises:
            ContextError: if attempting to instantiate for a directory that is already
                managed.
        """
        self._directory = pathlib.Path(directory).resolve()

        # TODO: Use file backing for journaled operations.
        self._log = []

        self._update_lock = threading.Lock()
        self._dirty = threading.Event()

        try:
            with _lock.scoped_directory_lock(self._directory):
                existing_filestore = FileStore._instances.get(self._directory, None)
                if existing_filestore is not None:
                    raise _exceptions.ContextError(f"{directory} is already managed by {repr(existing_filestore)}")
                # The current interpreter process is not aware of an instance for
                # *directory*

                for parent in self._directory.parents:
                    if _lock.is_locked(parent) or parent.joinpath(_data_subdirectory).exists():
                        raise _exceptions.ContextError(
                            f"Cannot establish scalems work directory {directory} "
                            f"because it is nested in work directory {parent}."
                        )

                instance_id = os.getpid()

                try:
                    self.datastore.mkdir()
                except FileExistsError:
                    # Check if the existing filesystem state is due to a previous clean
                    # shutdown, a previous dirty shutdown, or inappropriate concurrent
                    # access.
                    if not self.filepath.exists():
                        raise _exceptions.ContextError(
                            f"{self._directory} contains invalid datastore {self.datastore}."
                        )
                    logger.debug("Restoring metadata from previous session.")
                    with self.filepath.open() as fp:
                        metadata_dict = json.load(fp)
                    if metadata_dict["instance"] is not None:
                        # The metadata file has an owner.
                        if metadata_dict["instance"] != instance_id:
                            # This would be a good place to check a heart beat or
                            # otherwise
                            # try to confirm whether the previous owner is still active.
                            raise _exceptions.ContextError("Context is already in use.")
                        else:
                            assert metadata_dict["instance"] == instance_id
                            raise _exceptions.InternalError(
                                "This process appears already to be managing "
                                f"{self.filepath}, but is not "
                                "registered in FileStore._instances."
                            )
                    else:
                        # The metadata file has no owner. Presume clean shutdown
                        logger.debug(f"Taking ownership of metadata file for PID {instance_id}")
                        metadata_dict["instance"] = instance_id
                else:
                    logger.debug(f"Created new data store {self.datastore}.")
                    metadata_dict = {"instance": instance_id}

                metadata = Metadata(**metadata_dict)
                with open(self.filepath, "w") as fp:
                    json.dump(dataclasses.asdict(metadata), fp)
                FileStore._instances[self._directory] = self
                self.__dict__["_data"] = metadata

        except _lock.LockException as e:
            raise _exceptions.ContextError(
                "Could not acquire ownership of working directory {}".format(directory)
            ) from e
        self._repr = "<{cls} pid={pid} dir={dir}>".format(
            cls=self.__class__.__qualname__, pid=self.instance, dir=self.directory
        )
        assert FileStore._instances.get(self.directory) is self
        logger.debug(f"FileStore {self} open.")

    def flush(self):
        """Write the current metadata to the backing store, if there are pending
        updates.

        For atomic updates and better forensics, we write to a new file and *move* the
        file to replace the previous metadata file. If an error occurs, the temporary
        file will linger.

        Changes to metadata must be
        made with the update lock held and must end by setting the "dirty" condition
        (notifying the watchers of "dirty"). flush() should acquire the update lock and
        clear the "dirty" condition when successful. We can start with "dirty" as an
        Event. If we need more elaborate logic, we can use a Condition.
        It should be sufficient to use asyncio primitives, but we may need to use
        primitives from the `threading` module to allow metadata updates directly
        from RP task callbacks.

        """
        with self._update_lock:
            if self._dirty.is_set():
                # Warning: This can throw if, for instance, the parent directory disappears.
                with tempfile.NamedTemporaryFile(
                    dir=self.datastore, delete=False, mode="w", prefix="flush_", suffix=".json"
                ) as fp:
                    json.dump(dataclasses.asdict(self._data), fp)
                if self.closed:
                    logger.error(f"Leaving temporary metadata file {fp.name} from attempt to flush stale {self}.")
                    raise StaleFileStore("Cannot flush stale FileStore.")
                pathlib.Path(fp.name).rename(self.filepath)
                self._dirty.clear()

    def close(self):
        """Flush, shut down, and disconnect the FileStore from the managed directory.

        Not robustly thread safe. User is responsible for serializing access, as necessary,
        though ideally *close()* should be called exactly once, after the instance is no
        longer in use.

        Raises:
            StaleFileStore if called on an invalid or outdated handle.
            ScopeError if called from a disallowed context, such as from a forked process.

        """
        actual_manager = FileStore._instances.get(self._directory, None)
        if actual_manager is None:
            # Since FileStore._instances is a WeakValueDict, the entry may already be
            # gone if this call to `close` is happening during garbage collection.
            logger.debug(f"FileStore._instances[{self.directory}] is already empty")
        else:
            if actual_manager is not self:
                raise StaleFileStore(f"{self.directory} is currently managed by {actual_manager}")

        try:
            self.flush()
        except Exception as e:
            raise StaleFileStore(f"Could not flush {repr(self)}.") from e

        current_instance = getattr(self, "instance", None)
        self._data.instance = None

        try:
            with _lock.scoped_directory_lock(self.directory):
                if FileStore._instances.get(self._directory, None) is self:
                    del FileStore._instances[self._directory]
                if current_instance is None:
                    raise StaleFileStore(f"{self} is already closed.")
                elif current_instance != os.getpid():
                    raise _exceptions.ScopeError("Calling close() on a FileStore from another process is not allowed.")
                with open(self.filepath, "r") as fp:
                    context = json.load(fp)
                stored_instance = context.get("instance", None)
                if stored_instance != current_instance:
                    # Note that the StaleFileStore check will be more intricate as
                    # metadata becomes more sophisticated.
                    raise StaleFileStore(
                        "Expected ownership by {}, but found {}".format(current_instance, stored_instance)
                    )
                else:
                    context["instance"] = None
                    with open(self.filepath, "w") as fp:
                        json.dump(context, fp)
        except OSError as e:
            raise _exceptions.ContextError("Trouble with workflow directory access during shutdown.") from e

        logger.debug(f"FileStore {self} closed.")

    @property
    def closed(self) -> bool:
        if self._data.instance is None:
            actual_manager = FileStore._instances.get(self.directory, None)
            if actual_manager is self:
                raise _exceptions.ContextError(f"{self} is in an invalid state.")
            else:
                return True
        return False

    async def add_file(
        self, obj: typing.Union[_file.AbstractFile, _file.AbstractFileReference], _name: str = None
    ) -> FileReference:
        """Add a file to the file store.

        Not thread safe. User is responsible for serializing access, as necessary.

        We require file paths to be wrapped in a special type so that we can enforce
        that some error checking is possible before the coroutine actually runs. See

        This involves placing (copying) the file, reading the file to fingerprint it,
        and then writing metadata for the file. For clarity, we also rename the file
        after fingerprinting to remove a layer of indirection, even though this
        generates additional load on the filesystem.

        The caller is allowed to provide a *_name* to use for the filename,
        but this is strongly discouraged. The file will still be fingerprinted and the
        caller must be sure the key is unique across all software components that may
        by using the FileStore. (This use case should be limited to internal use or
        core functionality to minimize namespace collisions.)

        If the file is provided as a memory buffer or io.IOBase subclass, further
        optimization is possible to reduce the filesystem interaction to a single
        buffered write, but such optimizations are not provided natively by
        FileStore.add_file(). Instead, such optimizations may be provided by utility
        functions that produce AbstractFileReference objects that FileStore.add_file() can
        consume.

        Raises:
            scalems.exceptions.DuplicateKeyError: if a file with the same
                `ResourceIdentifier` is already under management.
            StaleFileStore: if the filestore state appears invalid.

        In addition to TypeError and ValueError for invalid inputs, propagates
        exceptions raised by failed attempts to access the provided file object.

        See Also:
            :py:func:`scalems.file.describe_file()`

        """
        # Note: We can use a more effective and flexible `try: ... except: ...` check
        # once we actually have AbstractFileReference support ready to use here, at which point
        # we can consider removing runtime_checkable from AbstractFileReference to avoid false
        # confidence. However, we may prefer to convert AbstractFileReference to an abc.ABC so
        # that we can actually functools.singledispatchmethod on the provided object.
        if isinstance(obj, _file.AbstractFileReference):
            raise _exceptions.MissingImplementationError("AbstractFileReference input not yet supported.")
        # Early exit check. We need to check again once we have a lock.
        if self.closed:
            raise StaleFileStore("Cannot add file to a closed FileStore.")
        path: pathlib.Path = pathlib.Path(obj)
        filename = None
        key = None
        _fd, tmpfile_target = tempfile.mkstemp(prefix=self._tmpfile_prefix, dir=self.datastore)
        os.close(_fd)
        tmpfile_target = pathlib.Path(tmpfile_target)
        try:
            # 1. Copy the file.
            kwargs = dict(src=path, dst=tmpfile_target, follow_symlinks=False)
            _path = await asyncio.to_thread(shutil.copyfile, **kwargs)
            assert tmpfile_target == _path
            assert tmpfile_target.exists()
            # We have now secured a copy of the file. We could just schedule the remaining
            # operations and return. For the initial implementation though, we will wait
            # until we have read the file. When optimizing for inputs that are already
            # fingerprinted, we could return before AbstractFileReference.localize() has
            # finished and schedule a checksum verification that localize() must wait for.

            # 2. Fingerprint the file.
            checksum = await asyncio.to_thread(obj.fingerprint, **{})
            identifier = _identifiers.ResourceIdentifier(checksum)

            # 3. Choose a key.
            key = str(identifier)

            # 4. Write metadata.
            if _name is None:
                _name = key
            filename = self.datastore.joinpath(_name)
            with self._update_lock:
                # Check again, now that we have the lock.
                if self.closed:
                    raise StaleFileStore("Cannot add file to a closed FileStore.")

                if key in self._data.files:
                    raise _exceptions.DuplicateKeyError(f"FileStore is already managing a file with key {key}.")
                if str(filename) in self._data.files.values():
                    raise _exceptions.DuplicateKeyError(f"FileStore already has {filename}.")
                else:
                    if filename.exists():
                        raise StaleFileStore(f"Unexpected file in filestore: {filename}.")
                os.rename(tmpfile_target, filename)
                logger.debug(f"Placed {filename}")
                # TODO: Enforce the setting of the "dirty" flag.
                # We can create a stronger separation between the metadata manager and
                # the backing data store and/or restrict assignment to use a general
                # Data Descriptor that manages our "dirty" flag.
                self._dirty.set()
                self._data.files[key] = str(filename)

            return FileReference(filestore=self, key=key)
        except (_exceptions.DuplicateKeyError, StaleFileStore) as e:
            tmpfile_target.unlink(missing_ok=True)
            raise e
        except Exception as e:
            # Something went particularly wrong. Let's try to clean up as best we can.
            logger.exception(f"Unhandled exception while trying to store {obj}")
            if key and key in self._data.files:
                pathlib.Path(self._data.files[key]).unlink(missing_ok=True)
                del self._data.files[key]
            if isinstance(filename, pathlib.Path):
                filename.unlink(missing_ok=True)
            raise e
        finally:
            if tmpfile_target.exists():
                logger.warning(f"Temporary file left at {tmpfile_target}")

    @property
    def files(self) -> typing.Mapping[_identifiers.ResourceIdentifier, pathlib.Path]:
        """Proxy to the managed files metadata.

        The proxy is read-only, but dynamic. A reference to *FileStore.files*
        will provide a dynamic view for later use.

        Warning:
            The returned FilesView will extend the lifetime of the metadata structure,
            but will not extend the lifetime of the FileStore itself.
        """
        if self.closed:
            raise StaleFileStore("FileStore is closed.")
        return FilesView(self._data.files)

    def add_task(self, identity: _identifiers.Identifier, **kwargs):
        """Add a new task entry to the metadata store.

        Not thread-safe. (We may need to manage this in a thread-safe manner, but it is
        probably preferable to do that by delegating a single-thread or thread-safe manager
        to serialize metadata edits.)
        """
        # Design note: among other interface requirements, the Metadata interface
        # should adapt between scalems types and serializable forms, so that str
        # conversions like the following become inappropriate.
        if str(identity) in self._data.objects:
            # TODO: It is not an error to add an item with the same key if it
            #  represents the same workflow object (potentially in a more or less
            #  advanced state).
            raise _exceptions.DuplicateKeyError(f"Task {identity} is already in the metadata store.")
        else:
            # Note: we don't attempt to be thread-safe, and this is not (yet) an async
            # coroutine, so we don't bother with a lock at this point.
            self._dirty.set()
            self._data.objects[str(identity)] = dict(**kwargs)
            # TODO: Take care with the value types that we can serialize and
            #  deserialize them.


class FileReference(_file.AbstractFileReference):
    """Provide access to a :py:class:`File <scalems.file.AbstractFile>` managed by a `FileStore`.

    Implements :py:class:`~scalems.file.AbstractFileReference`.
    """

    def __init__(self, filestore: FileStore, key: _identifiers.ResourceIdentifier | str):
        self._filestore: FileStore = filestore
        if key not in self._filestore.files:
            raise _exceptions.ProtocolError("Cannot create reference to unmanaged file.")
        if not isinstance(key, _identifiers.ResourceIdentifier):
            key = _identifiers.ResourceIdentifier(bytes.fromhex(key))
        self._key = key
        self._repr = f"<FileReference key:{key} path:{self._filestore.files[key]}>"

    def __fspath__(self) -> str:
        if not self.is_local():
            raise _file.DataLocalizationError(
                f"Non-local object (managed by {self._filestore}) is not implicitly convertible to filesystem path."
            )
        return str(self._filestore.files[self._key])

    def is_local(self, context=None) -> bool:
        if context is None:
            context = self._filestore
        return self._key in context.files

    async def localize(self, context=None) -> "FileReference":
        if context is None:
            context = self._filestore
        if context is not self._filestore:
            raise _exceptions.MissingImplementationError("Push subscription not yet implemented.")
        assert self._key in context.files
        return self

    def filestore(self) -> FileStore:
        return self._filestore

    def key(self) -> _identifiers.ResourceIdentifier:
        return self._key

    def as_uri(self, context=None) -> str:
        if context is None:
            context = self._filestore
        if context is not self._filestore:
            raise _exceptions.MissingImplementationError("Path resolution dispatching is not yet implemented.")
        if self.is_local():
            return context.files[self._key].as_uri()
        else:
            raise _exceptions.MissingImplementationError("No URI available for non-local files.")

    def __repr__(self):
        return self._repr


def filestore_generator(directory=None):
    """Generator function to manage a FileStore instance.

    Initializes a FileStore and yields (a weak reference to) it an arbitrary number of
    times. When the generator is garbage collected, or otherwise interrupted, properly
    closes the FileStore before finishing.

    This is a simple "coroutine" using the extended generator functionality from PEP 342.
    See Also:
        https://docs.python.org/3/howto/functional.html#passing-values-into-a-generator

    """
    if directory is None:
        directory = os.getcwd()
    path = pathlib.Path(directory)
    filestore = FileStore(directory=path)
    _closed = filestore.closed
    try:
        while not filestore.closed:
            message = yield weakref.ref(filestore)
            # We do not yet have any protocols requiring us to send messages to the
            # filestore when getting it.
            assert message is None
            # Note that we could also do interesting things with nested generators/coroutines
            # Ref PEP-0380: "return expr in a generator causes StopIteration(expr)
            #                to be raised upon exit from the generator."
        _closed = True
    finally:
        # The expected use case is that the generator will be abandoned when the owner
        # of the FileStoreManager is destroyed, resulting in
        # the Python interpreter calling the `close()` method, raising a GeneratorExit
        # exception at the above `yield`. We don't need to catch that explicitly. We
        # just need to clean up when the filestore is no longer in use.
        try:
            if not _closed:
                logger.debug(f"filestore_generator is closing {filestore}.")
                filestore.close()
        except Exception:
            # Note that this exception will probably be in the context of handling the
            # GeneratorExit exception from the `try` block.
            logger.exception(f"Exception while trying to close {filestore}.")
        else:
            assert filestore.closed


class FileStoreManager:
    def __init__(self, directory=None):
        self.filestore_generator = filestore_generator(directory=directory)
        # Make sure generator function runs up to the first yield.
        next(self.filestore_generator)

    def filestore(self) -> typing.Union[None, FileStore]:
        try:
            ref = next(self.filestore_generator)
            return ref()
        except StopIteration:
            logger.error("Managed FileStore is no longer available.")
            return None

    def close(self):
        if self.filestore_generator is None:
            return False
        else:
            store = self.filestore()
            if store is not None:
                store.close()
            try:
                next(self.filestore_generator)
            except StopIteration:
                logger.debug(f"{self} has been closed.")
            self.filestore_generator = None
            return True


async def _get_file_reference_by_path(obj: pathlib.Path, *, filestore: FileStore, mode="rb") -> FileReference:
    """Get file reference, adding to filestore, if and only if necessary.

    TODO: Optimize. (This call should only read and fingerprint *obj* once.)
    """
    obj = _file.describe_file(obj.resolve(), mode=mode)
    try:
        file_ref = await filestore.add_file(_file.describe_file(obj, mode=mode))
    except _exceptions.DuplicateKeyError:
        # Return the existing fileref, if it matches the fingerprint of *obj*.
        fingerprint = await asyncio.to_thread(obj.fingerprint)
        key = _identifiers.ResourceIdentifier(fingerprint)
        file_ref = filestore.get(key)
    return file_ref


@functools.singledispatch
def get_file_reference(obj, filestore=None, mode="rb") -> typing.Coroutine[None, None, FileReference]:
    """Get a FileReference for the provided object.

    If *filestore* is provided, use the given FileStore to manage the `FileReference`.
    Otherwise, use the FileStore for the current WorkflowManager.

    This is a dispatching function. Handlers for particular object types must are
    registered by decorating with ``@get_file_reference.register``. See
    :py:func:`functools.singledispatch`.

    Args:
        obj: identify the (managed) file
        filestore: file resource management backing store
        mode (str) : file access mode

    If *obj* is a :py:class:`~pathlib.Path` or :py:class:`~os.PathLike` object,
    add the identified file to the file store. If *obj* is a `ResourceIdentifier`,
    attempts to retrieve a reference to the identified filesystem object.

    Note:
        If *obj* is a `str` or :py:class:`bytes`, *obj* is interpreted as a `ResourceIdentifier`,
        **not** as a filesystem path.

    TODO: Either try to disambiguate str and bytes, or disallow and require stronger typed inputs.

    This involves placing (copying) the file, reading the file to fingerprint it,
    and then writing metadata for the file. For clarity, we also rename the file
    after fingerprinting to remove a layer of indirection, even though this
    generates additional load on the filesystem.

    In addition to TypeError and ValueError for invalid inputs, propagates
    exceptions raised by failed attempts to access the provided file object.

    TODO: Try to detect file type. See, for instance, https://pypi.org/project/python-magic/
    """
    try:
        return filestore.get_file_reference(obj, mode=mode)
    except AttributeError:
        # We don't mind if *filestore* does not provide this method.
        pass
    # We might expect *filestore* to raise NotImplemented or TypeError if it is
    # unable to handle the dispatch. This would not be an error in itself, except that
    # we do not have any other fall-back dispatching for types that have not been
    # registered.
    raise TypeError(f"Cannot convert {obj.__class__.__qualname__} to FileReference.")


@get_file_reference.register
def _(obj: pathlib.Path, filestore=None, mode="rb") -> typing.Coroutine[None, None, FileReference]:
    if filestore is None:
        raise _exceptions.MissingImplementationError(
            'There is not yet a "default" data store. *filestore* must be provided.'
        )
    else:
        # TODO: Check whether filestore is local or whether we need to proxy the object.
        ...

    # Should we assume that a Path object is intended to refer to a local file? We don't
    # want to  be ambiguous if the same path exists locally and remotely.
    if not obj.exists() or not obj.is_file():
        raise ValueError(f"Path {obj} is not a valid local file.")

    return _get_file_reference_by_path(obj, filestore=filestore, mode=mode)


@get_file_reference.register
def _(obj: str, *args, **kwargs) -> typing.Coroutine[None, None, FileReference]:
    return get_file_reference(_identifiers.ResourceIdentifier(bytes.fromhex(obj)), *args, **kwargs)


@get_file_reference.register
def _(obj: bytes, *args, **kwargs) -> typing.Coroutine[None, None, FileReference]:
    return get_file_reference(_identifiers.ResourceIdentifier(obj), *args, **kwargs)


@get_file_reference.register
def _(obj: os.PathLike, *args, **kwargs) -> typing.Coroutine[None, None, FileReference]:
    return get_file_reference(pathlib.Path(obj), *args, **kwargs)
