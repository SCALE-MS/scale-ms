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
"""

__all__ = [
    'ContextError',
    'StaleFileStore',
    'initialize_datastore',
]

import asyncio
import codecs
import contextvars
import dataclasses
import functools
import hashlib
import json
import locale
import logging
import os
import pathlib
import shutil
import tempfile
import threading
import typing
import warnings
import weakref
from contextvars import ContextVar
from typing import Iterator
from typing import Optional
from weakref import ReferenceType

import scalems.exceptions
from ._file import FileReference
from ._lock import is_locked
from ._lock import LockException
from ._lock import scoped_directory_lock as _scoped_directory_lock

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

_data_subdirectory = 'scalems_0_0'
"""Subdirectory name to use for managed filestore."""

_metadata_filename = 'scalems_context_metadata.json'
"""Name to use for Context metadata files (module constant)."""

_filestore: ContextVar[Optional[ReferenceType]] = contextvars.ContextVar('_filestore')


class StaleFileStore(Exception):
    """The backing file store is not consistent with the known (local) history."""


class ContextError(Exception):
    """A Context operation could not be performed."""


@dataclasses.dataclass
class Metadata:
    instance: int
    files: typing.MutableMapping[str, str] = dataclasses.field(
        default_factory=dict)


class FilesView(typing.Mapping[str, pathlib.Path]):
    """Read-only viewer for files metadata."""

    def __init__(self, files: typing.Mapping[str, str]):
        self._files = files

    def __getitem__(self, k: str) -> pathlib.Path:
        return pathlib.Path(self._files[k])

    def __len__(self) -> int:
        return len(self._files)

    def __iter__(self) -> Iterator[str]:
        for key in self._files:
            yield key


class FileStore:
    """Handle to the SCALE-MS nonvolatile data store for a workflow context.

    Not thread safe. User is responsible for serializing access, as necessary.
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

    @property
    def _tmpfile_prefix(self) -> str:
        return f'tmp_{self.instance}_'

    @property
    def log(self):
        # Interface TBD...
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
        file-backed data. Its format is closely related to the scalems API level.
        """
        if self._filepath is None:
            self._filepath = self.datastore / _metadata_filename
        return self._filepath

    @property
    def instance(self):
        return self._data.instance

    def __repr__(self):
        return f'<{self.__class__.__qualname__}, "{self.instance}:{self.directory}">'

    def __init__(self, *,
                 directory: pathlib.Path):
        """Assemble the data structure.

        Users should not create FileStore objects directly, but with
        initialize_datastore() or through the WorkflowManager instance.

        Once initialized, caller is responsible for calling the close() method either
        directly or by using the instance as a Python context manager (in a `with`
        expression).

        No directory in the filesystem should be managed by more than one FileStore.
        The FileStore class maintains a registry of instances to prevent instantiation of
        a new FileStore for a directory that is already managed.

        Raises:
            ContextError if attempting to instantiate for a directory that is already
            managed.
        """
        self._directory = pathlib.Path(directory).resolve()

        # TODO: Use a log file!
        self._log = []

        self._update_lock = threading.Lock()
        self._dirty = threading.Event()

        try:
            with _scoped_directory_lock(self._directory):
                existing_filestore = self._instances.get(self._directory, None)
                if existing_filestore:
                    raise ContextError(
                        f'{directory} is already managed by {repr(existing_filestore)}'
                    )
                # The current interpreter process is not aware of an instance for
                # *directory*

                for parent in self._directory.parents:
                    if is_locked(parent) or parent.joinpath(_data_subdirectory).exists():
                        raise ContextError(
                            f'Cannot establish scalems work directory {directory} '
                            f'because it is nested in work directory {parent}.')

                instance_id = os.getpid()

                try:
                    self.datastore.mkdir()
                except FileExistsError:
                    # Check if the existing filesystem state is due to a previous clean
                    # shutdown, a previous dirty shutdown, or inappropriate concurrent
                    # access.
                    if not self.filepath.exists():
                        raise ContextError(
                            f'{self._directory} contains invalid datastore '
                            f'{self.datastore}.'
                        )
                    logger.debug('Restoring metadata from previous session.')
                    with self.filepath.open() as fp:
                        metadata_dict = json.load(fp)
                    if metadata_dict['instance'] is not None:
                        # The metadata file has an owner.
                        if metadata_dict['instance'] != instance_id:
                            # This would be a good place to check a heart beat or
                            # otherwise
                            # try to confirm whether the previous owner is still active.
                            raise ContextError('Context is already in use.')
                        else:
                            assert metadata_dict['instance'] == instance_id
                            raise scalems.exceptions.InternalError(
                                'This process appears already to be managing '
                                f'{self.filepath}, but is not '
                                'registered in FileStore._instances.'
                            )
                    else:
                        # The metadata file has no owner. Presume clean shutdown
                        logger.debug(
                            f'Taking ownership of metadata file for PID {instance_id}')
                        metadata_dict['instance'] = instance_id
                else:
                    logger.debug(f'Created new data store {self.datastore}.')
                    metadata_dict = {'instance': instance_id}

                metadata = Metadata(**metadata_dict)
                with open(self.filepath, 'w') as fp:
                    json.dump(dataclasses.asdict(metadata), fp)
                FileStore._instances[self._directory] = self
                self.__dict__['_data'] = metadata

        except LockException as e:
            raise ContextError(
                'Could not acquire ownership of working directory {}'.format(
                    directory)) from e

    def __enter__(self):
        self._token = _filestore.set(weakref.ref(self))
        # TODO: Clarify reentrance behavior (global versus internal locking).
        # There shouldn't really be any problem with reentrance for regular `with`
        # block, though maybe we should confirm that we are in the same process and
        # thread. The complications should only arise with asyncronous `with` blocks
        # (i.e. `__aenter__`, `__aexit__`). If necessary, we can use a Semaphore to
        # count recursion depth across Contexts and/or cache a Semaphore value in a
        # ContextVar to watch for errors.
        if self._token.old_value in {self, contextvars.Token.MISSING}:
            return self
        else:
            _filestore.reset(self._token)
            del self._token
            raise ContextError(
                'FileStore is not reentrant'
            )

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.close()
        except (StaleFileStore, scalems.exceptions.ScopeError) as e:
            warnings.warn(f'{repr(self)} could not be exited cleanly.')
            logger.exception('FileStore.close() raised exception.', exc_info=e)
        else:
            _filestore.reset(self._token)
            del self._token
        # Indicate that we have not handled any exceptions.
        if exc_value:
            return False

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
                with tempfile.NamedTemporaryFile(dir=self.datastore,
                                                 delete=False,
                                                 mode='w',
                                                 prefix='flush_',
                                                 suffix='.json') as fp:
                    json.dump(dataclasses.asdict(self._data), fp)
                pathlib.Path(fp.name).rename(self.filepath)
                self._dirty.clear()

    def close(self):
        """Flush, shut down, and disconnect the FileStore from the managed directory.

        Raises:
            StaleFileStore if called on an invalid or outdated handle.
            ScopeError if called from a disallowed context, such as from a forked process.

        """
        current_instance = getattr(self, 'instance', None)
        if current_instance is None or self.closed:
            raise StaleFileStore(
                'Called close() on an inactive FileStore.'
            )
        if current_instance != os.getpid():
            raise scalems.exceptions.ScopeError(
                'Calling close() on a FileStore from another process is not allowed.')

        with _scoped_directory_lock(self.directory):
            try:
                with open(self.filepath, 'r') as fp:
                    context = json.load(fp)
                    stored_instance = context.get('instance', None)
                    if stored_instance != current_instance:
                        # Note that the StaleFileStore check will be more intricate as
                        # metadata becomes more sophisticated.
                        raise StaleFileStore(
                            'Expected ownership by {}, but found {}'.format(
                                current_instance,
                                stored_instance))
            except OSError as e:
                raise StaleFileStore('Could not open metadata file.') from e

            # del self._data.instance
            self._data.instance = None
            self._dirty.set()
            self.flush()
            del FileStore._instances[self.directory]

    @property
    def closed(self) -> bool:
        return FileStore._instances.get(self.directory, None) is not self

    async def add_file(self,
                       obj: typing.Union[str, pathlib.Path, os.PathLike, FileReference],
                       key: str = None,
                       mode='rb', encoding=None) -> FileReference:
        """Add a file to the file store.

        This involves placing (copying) the file, reading the file to fingerprint it,
        and then writing metadata for the file. For clarity, we also rename the file
        after fingerprinting to remove a layer of indirection, even though this
        generates additional load on the filesystem.

        The caller is allowed to provide a *key* to use for the filename instead of the
        calculated name, but the file will still be fingerprinted and the caller must
        be sure the key is unique. (This use case should be limited to internal use or
        core functionality to minimize namespace collisions.)

        If the file is provided as a memory buffer or io.IOBase subclass, further
        optimization is possible to reduce the filesystem interaction to a single
        buffered write, but such optimizations are not provided natively by
        FileStore.add_file(). Instead, such optimizations may be provided by utility
        functions that produce FileReference objects that FileStore.add_file() can
        consume.

        In addition to TypeError and ValueError for invalid inputs, propagates
        exceptions raised by failed attempts to access the provided file object.

        TODO: Move more of these checks to the task creation so we can catch errors
            earlier.
        """
        _mode = {
            'read': True,
            'write': False,
            'binary': True
        }
        if 'r' in mode:
            _mode['read'] = True
        else:
            raise ValueError('Only read-only file objects are currently supported.')
        if 'b' in mode:
            _mode['binary'] = True
        else:
            _mode['binary'] = False
            if encoding is None:
                encoding = locale.getpreferredencoding(False)
            try:
                codecs.getencoder(encoding)
            except LookupError:
                raise ValueError('Specify a valid character encoding for text files.')

        # Note: We can use a more effective and flexible `try: ... except: ...` check
        # once we actually have FileReference support ready to use here, at which point
        # we can consider removing runtime_checkable from FileReference to avoid false
        # confidence. However, we may prefer to convert FileReference to an abc.ABC so
        # that we can actually functools.singledispatchmethod on the provided object.
        if isinstance(obj, FileReference):
            raise scalems.exceptions.MissingImplementationError(
                'FileReference input not yet supported.')
        if isinstance(obj, (str, os.PathLike)):
            obj = pathlib.Path(obj)
        if not isinstance(obj, pathlib.Path):
            raise TypeError(f'{obj.__class__.__qualname__} could not be interpreted as '
                            f'a filesystem path.')
        path: pathlib.Path = obj.resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(f'Path {obj} is not a valid file.')

        try:
            from asyncio import to_thread
        except ImportError:
            from typing import Callable, Any
            _T = typing.TypeVar('_T')

            async def to_thread(__func: Callable[..., _T],
                                *args: Any,
                                **kwargs: Any) -> _T:
                """Mock Python to_thread for Py 3.8."""
                wrapped_function: Callable[[], _T] = functools.partial(__func, *args,
                                                                       **kwargs)
                assert callable(wrapped_function)
                loop = asyncio.get_event_loop()
                coro: typing.Awaitable[_T] = loop.run_in_executor(
                    None, wrapped_function)
                result = await coro
                return result

        filename = None
        tmpfile_target = self.datastore.joinpath(
            self._tmpfile_prefix + str(scalems.utility.next_monotonic_integer()))
        try:
            # 1. Copy the file.
            _path = await to_thread(
                shutil.copyfile, src=path, dst=tmpfile_target, follow_symlinks=False)
            assert tmpfile_target == _path
            assert tmpfile_target.exists()
            # We have now secured a copy of the file. We could just schedule the remaining
            # operations and return. For the initial implementation though, we will wait
            # until we have read the file. When optimizing for inputs that are already
            # fingerprinted, we could return before FileReference.localize() has
            # finished and schedule a checksum verification that localize() must wait for.

            # 2. Fingerprint the file.
            m = hashlib.sha256()
            if _mode['binary']:
                with open(tmpfile_target, 'rb') as f:
                    m.update(f)
            else:
                assert encoding
                with open(tmpfile_target, 'r', encoding=encoding) as f:
                    for line in f:
                        m.update(line.encode(encoding))
            checksum: bytes = m.digest()

            # 3. Choose a key.
            if key is None:
                key = checksum.hex()

            # 4. Write metadata.
            filename = self.datastore.joinpath(key)
            tmpfile_target.rename(filename)
            with self._update_lock:
                if key in self._data.files:
                    # TODO: Support caching. Use existing file if checksums match.
                    raise scalems.exceptions.DuplicateKeyError(
                        f'FileStore is already managing a file with key {key}.')
                self._data.files[key] = str(filename)

            return _FileReference(filestore=self, key=key)
        except Exception as e:
            logger.exception(f'Unhandled exception while trying to store {obj}',
                             exc_info=e)
            # Something went wrong. Let's try to clean up as best we can.
            if key and key in self._data.files:
                ...
            if isinstance(filename, pathlib.Path):
                filename.unlink(missing_ok=True)
            if tmpfile_target.exists():
                logger.warning(f'Temporary file left at {tmpfile_target}')

    @property
    def files(self) -> typing.Mapping[str, pathlib.Path]:
        """Proxy to the managed files metadata.

        Note that the view is frozen at the time of attribute lookup, not at the
        time of use, so a reference to FileStore.files will not provide a dynamic proxy
        for later use. This design point could be reconsidered, but seems consistent with
        SCALE-MS notions of scoped access.
        """
        if self.closed:
            raise StaleFileStore('FileStore is closed.')
        return FilesView(self._data.files)


class _FileReference(FileReference):
    def __init__(self, filestore: FileStore, key: str):
        self._filestore: FileStore = filestore
        if key not in self._filestore.files:
            raise scalems.exceptions.ProtocolError(
                'Cannot create reference to unmanaged file.')
        self._key = key

    def __fspath__(self) -> str:
        return str(self._filestore.files[self._key])

    def is_local(self, context=None) -> bool:
        if context is None:
            context = self._filestore
        return self._key in context.files

    async def localize(self, context=None) -> FileReference:
        if context is None:
            context = self._filestore
        if context is not self._filestore:
            raise scalems.exceptions.MissingImplementationError(
                'Push subscription not yet implemented.'
            )
        assert self._key in context.files
        return self

    def path(self, context=None) -> pathlib.Path:
        if context is None:
            context = self._filestore
        if context is not self._filestore:
            raise scalems.exceptions.MissingImplementationError(
                'Path resolution dispatching is not yet implemented.'
            )
        return context.files[self._key]

    def filestore(self):
        return self._filestore

    def key(self):
        return self._key

    def as_uri(self, context=None) -> str:
        if context is None:
            context = self._filestore
        if context is not self._filestore:
            raise scalems.exceptions.MissingImplementationError(
                'Path resolution dispatching is not yet implemented.'
            )
        return context.files[self._key].as_uri()


def get_context() -> typing.Union[FileStore, None]:
    """Get currently active workflow context, if any."""
    ref = _filestore.get(None)
    if ref is not None:
        filestore = ref()
        if filestore is None:
            # Prune dead weakrefs.
            _filestore.set(None)
        return filestore
    return None


def set_context(datastore: FileStore):
    """Set the active workflow context.

    We do not yet provide for holding multiple metadata stores open at the same time.

    Raises:
        ContextError if a FileStore is already active.

    """
    current_context = get_context()
    if current_context is not None:
        raise ContextError(f'The context is already active: {current_context}')
    else:
        ref = weakref.ref(datastore)
        _filestore.set(ref)


def initialize_datastore(directory=None) -> FileStore:
    """Get a reference to a workflow metadata store.

    If initialize_datastore() succeeds, the caller is responsible for calling the
    `close()` method of the returned instance before the interpreter exits,
    either directly or by using the instance as a Python context manager.
    """
    if directory is None:
        directory = os.getcwd()
    path = pathlib.Path(directory)
    filestore = FileStore(directory=path)
    return filestore


@functools.singledispatch
def get_file_reference(obj, filestore=None) -> typing.Awaitable[FileReference]:
    """Get a FileReference for the provided object.

    If *filestore* is provided, use the given FileStore to manage the FileReference.
    Otherwise, use the FileStore for the current WorkflowManager.

    This is a dispatching function. Handlers for particular object types must are
    registered by decorating with ``@get_file_reference.register``. See
    :py:decorator:`functools.singledispatch`.
    """
    try:
        return filestore.get_file_reference(obj)
    except AttributeError:
        # We don't mind if *filestore* does not provide this method.
        pass
    # We might expect *filestore* to raise NotImplemented or TypeError if it is
    # unable to handle the dispatch. This would not be an error in itself, except that
    # we do not have any other fall-back dispatching for types that have not been
    # registered.
    raise TypeError(f'Cannot convert {obj.__class__.__qualname__} to FileReference.')


@get_file_reference.register(pathlib.Path)
def _(obj, filestore=None) -> typing.Awaitable[FileReference]:
    """Add a file to the file store.

    This involves placing (copying) the file, reading the file to fingerprint it,
    and then writing metadata for the file. For clarity, we also rename the file
    after fingerprinting to remove a layer of indirection, even though this
    generates additional load on the filesystem.

    In addition to TypeError and ValueError for invalid inputs, propagates
    exceptions raised by failed attempts to access the provided file object.
    """
    if filestore is None:
        filestore = get_context()
    else:
        # TODO: Check whether filestore is local or whether we need to proxy the object.
        ...
    path: pathlib.Path = obj.resolve()
    # Should we assume that a Path object is intended to refer to a local file? We don't
    # want to  be ambiguous if the same path exists locally and remotely.
    if not path.exists() or not path.is_file():
        raise ValueError(f'Path {obj} is not a valid file.')
    # Bind a coroutine to a Context with the active filestore to avoid race conditions.
    with filestore:
        task = asyncio.create_task(filestore.add_file(path))
    return task


@get_file_reference.register(str)
def _(obj, filestore=None) -> typing.Awaitable[FileReference]:
    return get_file_reference(pathlib.Path(obj), filestore=filestore)


@get_file_reference.register(os.PathLike)
def _(obj, filestore=None) -> typing.Awaitable[FileReference]:
    return get_file_reference(os.fspath(obj), filestore=filestore)
