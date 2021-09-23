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

__all__ = (
    'ContextError',
    'StaleFileStore',
    'describe_file',
    'filestore_generator',
    'get_file_reference',
    'FileStore',
    'FileStoreManager'
)

import abc
import asyncio
import codecs
import contextvars
import dataclasses
import enum
import functools
import hashlib
import json
import locale
import logging
import mmap
import os
import pathlib
import shutil
import tempfile
import threading
import typing
import weakref
from typing import Iterator

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
        if not isinstance(k, str):
            raise TypeError('Key type must be str.')
        return pathlib.Path(self._files[k])

    def __contains__(self, o: object) -> bool:
        if not isinstance(o, str):
            raise TypeError('Key type must be str.')
        return super().__contains__(o)

    def __len__(self) -> int:
        return len(self._files)

    def __iter__(self) -> Iterator[str]:
        for key in self._files:
            yield key


class AccessFlags(enum.Flag):
    NO_ACCESS = 0
    READ = enum.auto()
    WRITE = enum.auto()


class AbstractFile(typing.Protocol):
    """Abstract base for file types.

    See :py:func:`describe_file`.
    """
    access: AccessFlags

    # _params: dict = None
    #
    # def __repr__(self):
    #     params = ', '.join([f'{key}={value}' for key, value in self._params.items()])
    #     return f'{self.__class__.__qualname__}({params})'

    @abc.abstractmethod
    def __fspath__(self) -> str:
        raise NotImplementedError

    def path(self) -> pathlib.Path:
        return pathlib.Path(os.fspath(self))

    def fingerprint(self) -> bytes:
        """Get a checksum or other suitable fingerprint for the file data."""
        with open(os.fspath(self), 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as data:
                m = hashlib.sha256(data)
        checksum: bytes = m.digest()
        return checksum

    # @abc.abstractmethod
    # def serialize(self) -> bytes:
    #     ...
    #
    # @abc.abstractmethod
    # @classmethod
    # def deserialize(cls, stream: bytes):
    #     ...


class BaseBinary(AbstractFile):
    """Base class for binary file types."""
    _path: pathlib.Path

    def __init__(self,
                 path: typing.Union[str, pathlib.Path, os.PathLike],
                 access: AccessFlags = AccessFlags.READ):
        self._path = pathlib.Path(path).resolve()
        self.access = access

    def __fspath__(self) -> str:
        return str(self._path)

    def path(self) -> pathlib.Path:
        return self._path


class BaseText(BaseBinary):
    """Base class for text file types."""
    encoding = locale.getpreferredencoding(False)

    def __init__(self, path, access: AccessFlags = AccessFlags.READ, encoding=None):
        if encoding is None:
            encoding = locale.getpreferredencoding(False)
        try:
            codecs.getencoder(encoding)
        except LookupError:
            raise ValueError('Specify a valid character encoding for text files.')
        self.encoding = encoding
        super().__init__(path=path, access=access)

    def fingerprint(self) -> bytes:
        m = hashlib.sha256()
        # Use universal newlines and utf-8 encoding for text file fingerprinting.
        with open(self._path, 'r', encoding=self.encoding) as f:
            # If we knew more about the text file size or line length, or if we already
            # had an open buffer, socket, or mmap, we could  optimize this in subclasses.
            for line in f:
                m.update(line.encode('utf8'))
        checksum: bytes = m.digest()
        return checksum


def describe_file(obj: typing.Union[str, pathlib.Path, os.PathLike],
                  mode='rb',
                  encoding: str = None,
                  file_type_hint=None) -> AbstractFile:
    """Describe an existing local file."""
    access = AccessFlags.NO_ACCESS
    if 'r' in mode:
        access |= AccessFlags.READ
        if 'w' in mode:
            access |= AccessFlags.WRITE
    else:
        # If neither 'r' nor 'w' is provided, still assume 'w'
        access |= AccessFlags.WRITE
    # TODO: Confirm requested access permissions.

    if file_type_hint:
        raise scalems.exceptions.MissingImplementationError(
            'Extensible file types not yet supported.')

    if 'b' in mode:
        if encoding is not None:
            raise TypeError('*encoding* argument is not supported for binary files.')
        file = BaseBinary(path=obj, access=access)
    else:
        file = BaseText(path=obj, access=access, encoding=encoding)

    if not file.path().exists():
        raise ValueError('Not an existing file.')

    return file


_T = typing.TypeVar('_T')


def get_to_thread() \
        -> typing.Callable[
            [
                typing.Callable[..., _T],
                typing.Tuple[typing.Any, ...],
                typing.Dict[str, typing.Any]
            ],
            typing.Coroutine[typing.Any, typing.Any, _T]]:
    """Provide a to_thread function.

    asyncio.to_thread() appears in Python 3.9, but we only require 3.8 as of this writing.
    """
    try:
        from asyncio import to_thread as _to_thread
    except ImportError:
        async def _to_thread(__func: typing.Callable[..., _T],
                             *args: typing.Any,
                             **kwargs: typing.Any) -> _T:
            """Mock Python to_thread for Py 3.8."""
            wrapped_function: typing.Callable[[], _T] = functools.partial(__func, *args,
                                                                          **kwargs)
            assert callable(wrapped_function)
            loop = asyncio.get_event_loop()
            coro: typing.Awaitable[_T] = loop.run_in_executor(
                None, wrapped_function)
            result = await coro
            return result
    return _to_thread


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
        return str(self._repr)

    def __init__(self, *,
                 directory: pathlib.Path):
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
        self._repr = '<{cls} pid={pid} dir={dir}>'.format(
            cls=self.__class__.__qualname__,
            pid=self.instance,
            dir=self.directory
        )
        assert FileStore._instances.get(self.directory) is self
        logger.debug(f'FileStore {self} open.')

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
                if self.closed:
                    logger.error(f'Leaving temporary metadata file {fp.name} from '
                                 f'attempt to flush stale {self}.')
                    raise StaleFileStore('Cannot flush stale FileStore.')
                pathlib.Path(fp.name).rename(self.filepath)
                self._dirty.clear()

    def close(self):
        """Flush, shut down, and disconnect the FileStore from the managed directory.

        Raises:
            StaleFileStore if called on an invalid or outdated handle.
            ScopeError if called from a disallowed context, such as from a forked process.

        """
        actual_manager = FileStore._instances.get(self._directory, None)
        if actual_manager is None:
            # Since FileStore._instances is a WeakValueDict, the entry may already be
            # gone if this call to `close` is happening during garbage collection.
            logger.debug(f'FileStore._instances[{self.directory}] is already empty')
        else:
            if actual_manager is not self:
                raise StaleFileStore(
                    f'{self.directory} is currently managed by {actual_manager}')

        self.flush()
        current_instance = getattr(self, 'instance', None)
        self._data.instance = None

        try:
            with _scoped_directory_lock(self.directory):
                if FileStore._instances.get(self._directory, None) is self:
                    del FileStore._instances[self._directory]
                if current_instance is None:
                    raise StaleFileStore(f'{self} is already closed.')
                elif current_instance != os.getpid():
                    raise scalems.exceptions.ScopeError(
                        'Calling close() on a FileStore from another process is not '
                        'allowed.')
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
                else:
                    context['instance'] = None
                    with open(self.filepath, 'w') as fp:
                        json.dump(context, fp)
        except OSError as e:
            raise ContextError(
                'Trouble with workflow directory access during shutdown.') from e

        logger.debug(f'FileStore {self} closed.')

    @property
    def closed(self) -> bool:
        if self._data.instance is None:
            actual_manager = FileStore._instances.get(self.directory, None)
            if actual_manager is self:
                raise ContextError(f'{self} is in an invalid state.')
            else:
                return True
        return False

    async def add_file(self,
                       obj: typing.Union[AbstractFile, FileReference],
                       _name: str = None) -> FileReference:
        """Add a file to the file store.

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
        functions that produce FileReference objects that FileStore.add_file() can
        consume.

        In addition to TypeError and ValueError for invalid inputs, propagates
        exceptions raised by failed attempts to access the provided file object.
        """
        # Note: We can use a more effective and flexible `try: ... except: ...` check
        # once we actually have FileReference support ready to use here, at which point
        # we can consider removing runtime_checkable from FileReference to avoid false
        # confidence. However, we may prefer to convert FileReference to an abc.ABC so
        # that we can actually functools.singledispatchmethod on the provided object.
        if isinstance(obj, FileReference):
            raise scalems.exceptions.MissingImplementationError(
                'FileReference input not yet supported.')
        if self.closed:
            raise StaleFileStore('Cannot add file to a closed FileStore.')
        path: pathlib.Path = obj.path()
        to_thread = get_to_thread()

        filename = None
        key = None
        tmpfile_target = self.datastore.joinpath(
            self._tmpfile_prefix + str(scalems.utility.next_monotonic_integer()))
        try:
            # 1. Copy the file.
            kwargs = dict(
                src=path, dst=tmpfile_target, follow_symlinks=False
            )
            _path = await to_thread(
                shutil.copyfile, **kwargs)
            assert tmpfile_target == _path
            assert tmpfile_target.exists()
            # We have now secured a copy of the file. We could just schedule the remaining
            # operations and return. For the initial implementation though, we will wait
            # until we have read the file. When optimizing for inputs that are already
            # fingerprinted, we could return before FileReference.localize() has
            # finished and schedule a checksum verification that localize() must wait for.

            # 2. Fingerprint the file.
            checksum = await to_thread(obj.fingerprint, **{})

            # 3. Choose a key.
            key = checksum.hex()

            # 4. Write metadata.
            if _name is None:
                _name = key
            filename = self.datastore.joinpath(_name)
            with self._update_lock:
                if key in self._data.files:
                    # TODO: Support caching. Use existing file if checksums match.
                    raise scalems.exceptions.DuplicateKeyError(
                        f'FileStore is already managing a file with key {key}.')
                if str(filename) in self._data.files.values():
                    raise scalems.exceptions.DuplicateKeyError(
                        f'FileStore already has {filename}.')
                else:
                    if filename.exists():
                        raise StaleFileStore(f'Unexpected file in filestore: {filename}.')
                os.rename(tmpfile_target, filename)
                logger.debug(f'Placed {filename}')
                self._data.files[key] = str(filename)

            return _FileReference(filestore=self, key=key)
        except Exception as e:
            logger.exception(f'Unhandled exception while trying to store {obj}',
                             exc_info=e)
            # Something went wrong. Let's try to clean up as best we can.
            if key and key in self._data.files:
                pathlib.Path(self._data.files[key]).unlink(missing_ok=True)
                del self._data.files[key]
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
        self._repr = f'<FileReference key:{key} path:{self._filestore.files[key]}>'

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
            message = (yield weakref.ref(filestore))
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
                logger.debug(f'filestore_generator is closing {filestore}.')
                filestore.close()
        except Exception as e:
            logger.exception(f'Exception while trying to close {filestore}.',
                             exc_info=e)
            # logger.debug(
            #     f'Exception while trying to close {filestore}: {e}'
            # )
        else:
            assert filestore.closed


class FileStoreManager:
    def __init__(self, directory=None):
        self.filestore_generator = filestore_generator(directory=directory)
        # Make sure generator function runs up to the first yield.
        next(self.filestore_generator)

    def filestore(self):
        try:
            ref = next(self.filestore_generator)
            return ref()
        except StopIteration:
            logger.error('Managed FileStore is no longer available.')
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
                logger.debug(f'{self} has been closed.')
            self.filestore_generator = None
            return True


@functools.singledispatch
def get_file_reference(obj, filestore=None, mode='rb')\
        -> typing.Awaitable[FileReference]:
    """Get a FileReference for the provided object.

    If *filestore* is provided, use the given FileStore to manage the FileReference.
    Otherwise, use the FileStore for the current WorkflowManager.

    This is a dispatching function. Handlers for particular object types must are
    registered by decorating with ``@get_file_reference.register``. See
    :py:decorator:`functools.singledispatch`.

    Set *text* to ``True`` for text files.

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
    raise TypeError(f'Cannot convert {obj.__class__.__qualname__} to FileReference.')


@get_file_reference.register(pathlib.Path)
def _(obj, filestore=None, mode='rb') -> typing.Awaitable[FileReference]:
    """Add a file to the file store.

    This involves placing (copying) the file, reading the file to fingerprint it,
    and then writing metadata for the file. For clarity, we also rename the file
    after fingerprinting to remove a layer of indirection, even though this
    generates additional load on the filesystem.

    In addition to TypeError and ValueError for invalid inputs, propagates
    exceptions raised by failed attempts to access the provided file object.
    """
    if filestore is None:
        raise scalems.exceptions.MissingImplementationError(
            'There is not yet a "default" data store. *filestore* must be provided.'
        )
    else:
        # TODO: Check whether filestore is local or whether we need to proxy the object.
        ...

    path: pathlib.Path = obj.resolve()
    # Should we assume that a Path object is intended to refer to a local file? We don't
    # want to  be ambiguous if the same path exists locally and remotely.
    if not path.exists() or not path.is_file():
        raise ValueError(f'Path {obj} is not a valid file.')

    task = asyncio.create_task(
        filestore.add_file(describe_file(path, mode=mode)))
    return task


@get_file_reference.register(str)
def _(obj, *args, **kwargs) -> typing.Awaitable[FileReference]:
    return get_file_reference(pathlib.Path(obj), *args, **kwargs)


@get_file_reference.register(os.PathLike)
def _(obj, *args, **kwargs) -> typing.Awaitable[FileReference]:
    return get_file_reference(os.fspath(obj), *args, **kwargs)
