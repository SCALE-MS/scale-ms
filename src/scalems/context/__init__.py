"""Workflow scope and execution environment context management.

SCALE-MS optimizes data flow and data locality in part by attributing all
workflow references to well-defined scopes. Stateful API facilities, workflow
state, and scoped references are managed as WorkflowContext instances.

"Workflow Manager" is an important parameter in several cases.
Execution management tools like scalems.run(), scalems.wait(), and scalems.dispatch()
update the workflow managed in a particular scope, possibly by interacting with
other scopes. Commands for declaring work or data add items to specific instances
of workflow managers. Internally, SCALEMS explicitly refers to theses scopes as
*manager*, or *context*, depending on the nature of the code collaboration.
*manager* may appear as an optional parameter for user-facing functions to allow
a particular managed workflow to be specified.

This module supports scoped_context() and get_context() with internal module state.
These tools interact with the context management of the asynchronous dispatching,
but note that they are not thread-safe. scoped_context() should not be used in
a coroutine except in the root coroutine of a Task or otherwise within the scope
of a contextvars.copy_context().run(). scalems will try to flag misuse by raising
a ProtocolError, but please be sensible.
"""

__all__ = ['WorkflowManager']

import abc
import asyncio
import contextlib
import contextvars
import dataclasses
import functools
import json
import logging
import queue as _queue
import typing
import warnings
import weakref

from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import DuplicateKeyError
from scalems.exceptions import InternalError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.exceptions import ScaleMSError
from scalems.exceptions import ScopeError
from scalems.serialization import encode

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

# Identify an asynchronous Context. Non-asyncio-aware functions may need to behave
# differently when we know that asynchronous context switching could happen.
# We allow multiple dispatchers to be active, but each dispatcher must
# 1. contextvars.copy_context()
# 2. set itself as the dispatcher in the new Context.
# 3. run within the new Context.
# 4. ensure the Context is destroyed (remove circular references)
_dispatcher = contextvars.ContextVar('_dispatcher')


class QueueItem(dict, typing.MutableMapping[str, typing.Union[str, bytes]]):
    """Queue items are either workflow items or control messages."""

    def _hexify(self):
        for key, value in self.items():
            if isinstance(value, bytes):
                value = value.hex()
            yield key, value

    def __str__(self) -> str:
        return str(dict(self._hexify()))


class _CommandQueueControlItem(QueueItem, typing.MutableMapping[str, str]):
    """String-encoded Command for the Executor command queue.

    Currently the only supported key is "command".

    Currently the only supported value is "stop".
    """
    _allowed: typing.ClassVar = {'command': {'stop'}}

    def __setitem__(self, k: str, v: str) -> None:
        if k in self._allowed:
            if v in self._allowed[k]:
                super().__setitem__(k, v)
            else:
                raise APIError(f'Unsupported command value: {repr(v)}')
        else:
            raise APIError(f'Unsupported command key: {repr(k)}')


class _CommandQueueAddItem(QueueItem, typing.MutableMapping[str, bytes]):
    """String-encoded add_item command for the Executor command queue.
    """
    _allowed: typing.ClassVar = {'add_item'}

    def __setitem__(self, k: str, v: str) -> None:
        if k in self._allowed:
            if isinstance(v, bytes):
                super().__setitem__(k, v)
            else:
                raise APIError(f'Unsupported add_item key: {repr(v)}')
        else:
            raise APIError(f'Unsupported command: {repr(k)}')


class ResourceType:
    def identifier(self) -> str:
        """Resource type identifier"""
        return '.'.join(self._identifier)

    def as_tuple(self) -> tuple:
        return self._identifier

    def __init__(self, type_identifier: tuple):
        if not isinstance(type_identifier, tuple):
            raise TypeError('Expected a tuple of identifier strings.')
        # TODO: format checking.
        self._identifier = tuple([element for element in type_identifier])


class Description:
    def shape(self) -> tuple:
        return self._shape

    def type(self) -> ResourceType:
        return self._type

    def __init__(self, resource_type: ResourceType, *, shape: tuple = (1,)):
        # TODO: input validation
        self._shape = shape
        self._type = resource_type


class CommandResourceType(ResourceType):
    def input_description(self) -> Description:
        return Description(self._input_description.type(),
                           shape=self._result_description.shape())

    def result_description(self) -> Description:
        return Description(self._result_description.type(),
                           shape=self._result_description.shape())

    def __init__(self, *args, inputs: Description, results: Description, **kwargs):
        super().__init__(*args, **kwargs)
        self._input_description = Description(inputs.type(), shape=inputs.shape())
        self._result_description = Description(results.type(), shape=results.shape())


# TODO: class AbstractWorkflowItemView(abc.ABC):


class ItemView:
    """Standard object returned by a WorkflowContext when adding details to the workflow.

    Provides the normative interface for workflow items as a user-space object
    that proxies access through a workflow manager.

    Provides a Future like interface.

    At least in the initial implementation, a ItemView does not extend the lifetime
    of the Context to which it refers. If the Context from which the ItemView was
    obtained goes out of scope or otherwise becomes invalid, some ItemView interfaces
    can raise exceptions.

    .. todo:: Allows proxied access to future results through attribute access.

    """

    def uid(self) -> bytes:
        """Get the canonical unique identifier for this task.

        The identifier is universally unique and can be used to query any
        workflow manager for awareness of the task and (if the context is aware
        of the task) to get a view of the task.

        Returns:
            256-bit binary digest as a 32 element byte sequence.
        """
        return bytes(self._uid)

    def done(self) -> bool:
        """Check the status of the task.

        Returns:
            true if the task has finished.

        """
        context: WorkflowManager = self._context()
        if context is None:
            raise ScopeError('Out of scope. Managing context no longer exists!')
        return context.tasks[self.uid()].done()

    def result(self):
        """Get a local object of the tasks's result type.

        .. todo:: Forces dependency resolution.

        """
        context: WorkflowManager = self._context()
        if context is None:
            raise ScopeError('Out of scope. Managing context no longer exists!')
        # TODO: What is the public interface to the tasks or completion status?
        # Note: We want to keep the View object as lightweight as possible, such
        # as storing just the weak ref to the manager, and the item identifier.
        return context.tasks[self.uid()].result()

    def description(self) -> Description:
        """Get a description of the resource type."""
        context: WorkflowManager = self._context()
        if context is None:
            raise ScopeError('Out of scope. Managing context no longer exists!')
        return context.tasks[self.uid()].description()

    def __getattr__(self, item):
        """Proxy attribute accessor for special task members.

        If the workflow element provides the requested interface, the managing
        Context will provide appropriate access. For "result" attributes, the
        returned reference supports the Future interface.
        """
        # We don't actually want to do this check here, but this is essentially what
        # needs to happen:
        #     assert hasattr(self.description().type().result_description().type(), item)
        context: WorkflowManager = self._context()
        if context is None:
            raise ScopeError('Out of scope. Managing context no longer available!')
        task = context.tasks[self.uid()]  # type: Task
        try:
            return getattr(task, item)
        except KeyError as e:
            raise e

    def __init__(self, context, uid: bytes):
        self._context = weakref.ref(context)
        if isinstance(uid, bytes) and len(uid) == 32:
            self._uid = uid
        else:
            raise ProtocolError('uid should be a 32-byte binary digest (bytes). '
                                f'Got {uid}')


class WorkflowView:
    """Middleware interface for interacting with managed workflow items.

    This interface exists to compartmentalize access methods to the managed workflow,
    keeping the public WorkflowContext interface as simple as possible. The interfaces
    may be combined in the future.
    """


class WorkflowItemRecord:
    """Encapsulate the management of an item record in a BasicWorkflowManager."""


class BasicWorkflowManager:
    """Reference implementation for a workflow manager.

    Support addition and querying of items.
    """

    def __init__(self):
        self._items = dict()


class InvalidStateError(ScaleMSError):
    """Object state is not compatible with attempted access.

    May result from a temporarily unserviceable request or an inappropriate
    usage. Emitter should provide a specific message.

    This exception serves a similar function to asyncio.InvalidStateError.
    """


class Task:
    """Encapsulate the implementation details of a managed Task workflow item.

    * Provide the interface required to support ItemView.
    * Wrap the task execution handling details.

    Note: We need an interface to describe the return value of manager.item().
    We currently manage tasks internally via Task instances, but that is a detail
    that is subject to change without warning.

    Warning:
        Not thread-safe. Access should be mediated by WorkflowManager protocols
        within the asyncio event loop.

    Once Task objects are created, they may not be updated directly by the client.

    TODO:
        Really, this can and should be implemented in terms of asyncio.Task with
        some additional SCALEMS overlay.

    State machine:
        Initial (no coroutine) -> Dispatched (coroutine) -> Executing (Task or Future)
        -> Final (awaited)

    TODO:
        Allow composable details. A WorkflowManager can provide its own task factory,
        but it is convenient to provide a complete base implementation, and extension
        is likely limited to how the task is dispatched and how the Futures are
        fulfilled, which seem very pluggable.
    """

    def result(self):
        if not self.done():
            raise InvalidStateError('Called result() on a Task that is not done.')
        return self._result

    def description(self) -> Description:
        decoded_record = json.loads(self._serialized_record)
        resource_type = ResourceType(tuple(decoded_record['type']))
        # TODO: Handle multidimensional references.
        shape = (1,)
        value = Description(resource_type=resource_type, shape=shape)
        return value

    def uid(self) -> bytes:
        if not isinstance(self._uid, bytes):
            raise InternalError('Task._uid was stored as bytes. Implementation changed?')
        return bytes(self._uid)

    def done(self) -> bool:
        return self._done.is_set()

    def __getattr__(self, item):
        # TODO: Manage internal state updates.
        # TODO: Respect different attribute type semantics.
        try:
            decoded_record = json.loads(self._serialized_record)
            logger.debug('Decoded {}: {}'.format(type(decoded_record).__name__,
                                                 str(decoded_record)))
            value = decoded_record[item]
        except json.JSONDecodeError as e:
            raise AttributeError('Problem retrieving "{}"'.format(item)) from e
        except KeyError as e:
            logger.debug('Did not find "{}" in {}'.format(item, self._serialized_record))
            raise AttributeError('Problem retrieving "{}"'.format(item)) from e
        return value

    def __init__(self, context: 'WorkflowManager', record: str):
        self._serialized_record = str(record)
        decoded_record = json.loads(self._serialized_record)

        self._uid = bytes.fromhex(decoded_record['uid'])
        if not len(self._uid) == 256 // 8:
            raise ProtocolError('UID is supposed to be a 256-bit hash digest. '
                                f'Got {repr(self._uid)}')
        self._done = asyncio.Event()
        self._result = None

        # As long as we are storing Tasks in the context, we cannot store contexts in
        # Tasks.
        self._context = weakref.ref(context)

    def set_result(self, result):
        # Not thread-safe.
        if self._done.is_set():
            raise ProtocolError('Result is already set for {}.'.format(repr(self)))
        self._result = result
        self._done.set()
        logger.debug('Result set for {} in {}'.format(self.uid().hex(),
                                                      str(self._context())))

    # @classmethod
    # def deserialize(cls, context, record: str):
    #     item_view = context.add_item(record)
    #     return item_view

    def serialize(self):
        return self._serialized_record


# USE SINGLEDISPATCHMETHOD?

# TODO: Incorporate into WorkflowContext interface.
# Design note: WorkflowContext classes could cache implementation details for item types,
# but (since we acknowledge that work may be defined before instantiating the context to
# which it will be dispatched), we need to allow the WorkflowContext implementation to
# negotiate/fetch the implementation details at any time.
# In general, relationships between specific
# workflow item types and context types should be resolved in terms of context traits,
# not specific class definitions.
# In practice, we have some more tightly-coupled relationships,
# at least in early versions, as we handle
# (a) subprocess-type items,
# (b) function-based items,
# and (c) data staging details for (1) local execution and (2) remote (RADICAL Pilot)
# execution.
@functools.singledispatch
def workflow_item_director_factory(item, *, context, label: str = None) -> \
        typing.Callable[..., ItemView]:
    """

    Get a workflow item director for a context and input type.

    When called, the director finalizes the new item and returns a view.
    """
    raise MissingImplementationError('No registered implementation for {} in {}'.format(
        repr(item),
        repr(context)))


# TODO: Do we really want to handle dispatching on type _or_ instance args?
@workflow_item_director_factory.register
def _(item_type: type, *, context, label: str = None) -> typing.Callable[..., ItemView]:
    # When dispatching on a class instead of an instance, just construct an
    # object of the indicated type and re-dispatch. Note that implementers of
    # item types must register an overload with this singledispatch function.
    # TODO: Consider how best to register operations and manage their state.
    def constructor_proxy_director(*args, **kwargs) -> ItemView:
        if not isinstance(item_type, type):
            raise ProtocolError(
                'This function is intended for a dispatching path on which *item_type* '
                'is a `type` object.')
        item = item_type(*args, **kwargs)
        director = workflow_item_director_factory(item, context=context, label=label)
        return director()

    return constructor_proxy_director


class WorkflowEditor(abc.ABC):
    @abc.abstractmethod
    def add_item(self, item) -> ItemView:
        ...


class TaskMap(dict, typing.MutableMapping[bytes, Task]):
    def __setitem__(self, k: bytes, v: Task) -> None:
        if not isinstance(k, bytes) and isinstance(k, typing.SupportsBytes):
            k = bytes(k)
        if not isinstance(k, bytes) or not isinstance(v, Task):
            raise TypeError('TaskMap maps *bytes* keys to *Task* values.')
        super().__setitem__(k, v)


_QItem_T = typing.TypeVar('_QItem_T', bound=QueueItem)
AddItemCallback = typing.Callable[[_QItem_T], None]

_BackendT = typing.TypeVar('_BackendT', contravariant=True)


class RuntimeManager(typing.Generic[_BackendT], abc.ABC):
    """Client side manager for dispatching work loads and managing data flow."""
    session = None
    source_context: 'WorkflowManager'
    submitted_tasks: typing.List[asyncio.Task]

    _command_queue: asyncio.Queue
    _dispatcher_lock: asyncio.Lock
    _queue_runner_task: asyncio.Task = None

    def __init__(self,
                 source: 'WorkflowManager',
                 *,
                 loop: asyncio.AbstractEventLoop,
                 runtime: _BackendT,
                 dispatcher_lock=None):
        self.submitted_tasks = []
        self._finalizer = None

        # TODO: Only hold a queue in an active context manager.
        self._command_queue = asyncio.Queue()
        self._exception = None
        self.source_context = source
        self._loop: asyncio.AbstractEventLoop = loop
        self.configuration = runtime
        if not isinstance(dispatcher_lock, asyncio.Lock):
            raise TypeError('An asyncio.Lock is required to control dispatcher state.')
        self._dispatcher_lock = dispatcher_lock

    def runtime_shutdown(self, session):
        """Shutdown hook for runtime sessions.

        Called while exiting the context manager. Allows specialized handling of
        runtime backends in terms of the RuntimeManager instance and an abstract
        *session* object, presumably acquired while entering the context manager
        through the *runtime_startup* hook.
        """
        pass

    def queue(self):
        # TODO: Only expose queue while in an active context manager.
        return self._command_queue

    def active(self) -> bool:
        session = self.session
        if session is None:
            return False
        else:
            assert session is not None
            return not session.closed

    def __del__(self):
        if self.active():
            warnings.warn('{} was not explicitly shutdown.'.format(repr(self)))

    def exception(self) -> typing.Union[None, Exception]:
        return self._exception

    @contextlib.contextmanager
    def runtime_configuration(self):
        """Runtime startup context manager hook.

        This allows arbitrary environment changes during the scope of the __aenter__()
        function, as well as a chance to modify the stored runtime configuration object.

        Warning: this is probably not a permanent part of the interface. We should settle
        on whether to store a configuration/context object or rely on the current
        contextvars.Context.
        """
        ...
        try:
            yield None
        finally:
            ...

    @abc.abstractmethod
    def runtime_startup(self, runner_started: asyncio.Event) -> asyncio.Task:
        """Runtime startup hook.

        If the runtime manager uses a Session, this is the place to acquire it.

        Arguments:
            runner_started: an Event to be set as the runtime becomes ready to process
            its queue.

        The *runner_started* argument allows inexpensive synchronization. The caller may
        yield until until the runtime queue processing task has run far enough to be
        well behaved if it later encounters an error or is subsequently canceled.
        """
        ...

    @abc.abstractmethod
    def updater(self) -> 'AbstractWorkflowUpdater':
        """Initialize a WorkflowUpdater for the configured runtime."""
        # TODO: Convert from an abstract method to a registration pattern.
        ...

    async def __aenter__(self):
        with self.runtime_configuration():
            try:
                # Get a lock while the state is changing.
                # Warning: The dispatching protocol is immature.
                # Initially, we don't expect contention for the lock, and if there is
                # contention, it probably represents an unintended race condition
                # or systematic dead-lock.
                # TODO: Clarify dispatcher state machine and remove/replace assertions.
                assert not self._dispatcher_lock.locked()
                async with self._dispatcher_lock:
                    runner_started = asyncio.Event()
                    runner_task = self.runtime_startup(runner_started)
                    is_started = asyncio.create_task(runner_started.wait())
                    done, pending = await asyncio.wait((is_started, runner_task),
                                                       return_when=asyncio.FIRST_COMPLETED)
                    if runner_task in done:
                        logger.error('Runner task stopped early.')
                        if runner_task.cancelled():
                            raise DispatchError('Runner unexpectedly canceled while '
                                                'starting dispatching.')
                        else:
                            e = runner_task.exception()
                            logger.exception('Runner task failed with an exception.',
                                             exc_info=e)
                            raise e
                    self._queue_runner_task = runner_task

                # Note: it would probably be most useful to return something with a
                # WorkflowManager interface...
                return self
            except Exception as e:
                self._exception = e
                raise e

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: C901
        """Clean up at context exit.

        In addition to handling exceptions, clean up any Session resource.

        We also need to make sure that we properly disengage from any queues
        or generators.

        We can also leave flags for ourself to be checked at __await__,
        if there is a Task associated with the Executor.
        """
        # Note that this coroutine could take a long time and could be
        # cancelled at several points.
        cancelled_error = None
        # The dispatching protocol is immature.
        # Initially, we don't expect contention for the lock, and if there is
        # contention, it probably represents an unintended race condition
        # or systematic dead-lock.
        # TODO: Clarify dispatcher state machine and remove/replace assertions.
        assert not self._dispatcher_lock.locked()
        async with self._dispatcher_lock:
            session = self.session
            # This method is not thread safe, but we try to make clear as early as
            # possible that instance.session is no longer publicly available.
            self.session = None

            try:
                # Stop the executor.
                logger.debug('Stopping workflow execution.')
                # TODO: Make sure that nothing else will be adding items to the
                #  queue from this point.
                # We should establish some assurance that the next line represents
                # the last thing that we will put in the queue.
                self._command_queue.put_nowait({'control': 'stop'})
                # TODO: Consider what to do differently when we want to cancel work
                #  rather than just finalize it.

                # Currently, the queue runner does not place subtasks,
                # so there is only one thing to await.
                # TODO: We should probably allow the user to provide some sort of timeout,
                #  or infer one from other time limits.
                try:
                    await self._queue_runner_task
                except asyncio.CancelledError as e:
                    raise e
                except Exception as queue_runner_exception:
                    logger.exception('Unhandled exception when stopping queue handler.',
                                     exc_info=True)
                    self._exception = queue_runner_exception
                else:
                    logger.debug('Queue runner task completed.')
                finally:
                    if not self._command_queue.empty():
                        logger.error('Command queue never emptied.')

                    # Wait for the watchers.
                    if len(self.submitted_tasks) > 0:
                        results = await asyncio.gather(*self.submitted_tasks)
                        # TODO: Log something useful about the results.
                        assert len(results) == len(self.submitted_tasks)

                    self.runtime_shutdown(session)
            except asyncio.CancelledError as e:
                logger.debug(f'{self.__class__.__qualname__} context manager received '
                             f'cancellation while exiting.')
                cancelled_error = e
            except Exception as e:
                logger.exception('Exception while stopping dispatcher.', exc_info=True)
                if self._exception:
                    logger.error('Queuer is already holding an exception.')
                else:
                    self._exception = e
            finally:
                # Should we do any other clean-up here?
                # self.pilot = None
                # self._pilot_manager = None
                if session is not None:
                    if not _session_is_closed(session):
                        logger.debug(f'Queuer is shutting down {session}.')
                        try:
                            # Note: close() is not always fast, and we might want
                            # to do it asynchronously in the future.
                            session.close()
                        except asyncio.CancelledError as e:
                            cancelled_error = e
                        except Exception as e:
                            logger.exception(
                                'Exception during Session.close().',
                                exc_info=e)
                        else:
                            logger.debug('Runtime Session closed.')
        if cancelled_error:
            raise cancelled_error

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False

    # We don't currently have a use for a stand-alone Task.
    # We use the async context manager and the exception() method.
    # def __await__(self):
    #     """Implement the asyncio task represented by this object."""
    #     # Note that this is not a native coroutine object; we cannot `await`
    #     # The role of object.__await__() is to return an iterator that will be
    #     # run to exhaustion in the context of an event loop.
    #     # We assume that most of the asyncio activity happens through the
    #     # async context mananager behavior and other async member functions.
    #     # If we choose to `await instance` at all, we need a light-weight
    #     # iteration we can perform to surrender control of the event loop,
    #     # and then just do some sort of tidying or reporting that doesn't fit well
    #     # into __aexit__(), such as the ability to return a value.
    #
    #     # # Note: We can't do this without first wait on some sort of Done event...
    #     # failures = []
    #     # for t in self.rp_tasks:
    #     #     logger.info('%s  %-10s : %s' % (t.uid, t.state, t.stdout))
    #     #     if t.state != rp.states.DONE or t.exit_code != 0:
    #     #         logger.error(f'RP Task unsuccessful: {repr(t)}')
    #     #         failures.append(t)
    #     # if len(failures) > 0:
    #     #     warnings.warn('Unsuccessful tasks: ' + ', '.join([repr(t) for t in failures]))
    #
    #     yield
    #     if self._exception:
    #         raise self._exception
    #     return self.scheduler
    #
    #     # # If we want to provide a "Future-like" interface, we should support the callback
    #     # # protocols and implement the following generator function.
    #     # if not self.done():
    #     #     self._asyncio_future_blocking = True
    #     #     # ref https://docs.python.org/3/library/asyncio-future.html#asyncio.isfuture
    #     #
    #     #     yield self  # This tells Task to wait for completion.
    #     # if not self.done():
    #     #     raise RuntimeError("The dispatcher task was not 'await'ed.")
    #     # Ref PEP-0380: "return expr in a generator causes StopIteration(expr)
    #     # to be raised upon exit from the generator."
    #     # The Task works like a `result = yield from awaitable` expression.
    #     # The iterator (generator) yields until exhausted,
    #     # then raises StopIteration with the value returned in by the generator function.
    #     # return self.result()  # May raise too.
    #     # # Otherwise, the only allowed value from the iterator is None.


class AbstractWorkflowUpdater(abc.ABC):
    @abc.abstractmethod
    async def submit(self, *, item: Task) -> asyncio.Task:
        """Submit work to update a workflow item.

        Args:
            item: managed workflow item as produced by WorkflowManager.edit_item()

        Returns:
            watcher task for a coroutine that has already started.

        The returned Task is a watcher task to confirm completion of or allow
        cancellation of the dispatched work. Additional hidden Tasks may be
        responsible for updating the provided item when results are available,
        so the result of this task is not necessarily directly interesting to
        the execution manager.

        The caller check for unexpected cancellation or early failure. If the task has
        not failed or been canceled by the time *submit()* returns, then we can
        conclude that the task has been appropriately bound to the workfow item.
        """
        ...

def _session_is_closed(session):
    """Generic check for status of a session instance."""
    if not hasattr(session, 'closed'):
        raise TypeError('Bad argument. *session* does not report its open/closed status.')
    if callable(session.closed):
        return session.closed()
    else:
        # For example, radical.pilot.Session.closed is a `property`.
        return session.closed


class ExecutorFactory(typing.Protocol[_BackendT]):
    def __call__(self,
                 manager: 'WorkflowManager',
                 params: typing.Optional[_BackendT] = None) -> RuntimeManager[_BackendT]:
        ...


class WorkflowManager:
    """Composable context for SCALE-MS workflow management.

    A workflow manager includes a strategy for dispatching a workflow
    for execution. Instances provide the concurrent.futures.Executor
    interface with support and semantics that depend on the Executor
    implementation and execution environment.

    Notably, we rely on the Python contextmanager protocol to regulate
    the acquisition and release of resources, so SCALE-MS workflow
    contexts do not initialize Executors at creation. Instead,
    client code should use `with` blocks for scoped initialization and
    *shutdown* of Executor roles.

    TODO: Enforce centralization of Context instantiation for the interpreter process.
    For instance:
    * Implement a root context singleton and require acquisition of new Context
      handles through methods in this module.
    * Use abstract base class machinery to register Context implementations.
    * Require Context instances to track their parent Context, or otherwise
      participate in a single tree structure.
    * Prevent instantiation of Command references without a reference to a Context
      instance.

    TODO:
        Check that I'm actually toggling something for the context instance to avoid
        recursive dispatch loops rather than just multiple recursion of self.
        Maybe keep a reference to the context hierarchy node to use when entering,
        and let implementations decide whether to allow multiple entrance,
        provided there is a reasonable way to clean up
        the correct number of times.

    TODO:
        In addition to adding callbacks to futures, allow subscriptions to workflow
        updates.
        This allows intermediate updates to be propagated and could be a superset of
        the additem hook.

    """
    tasks: TaskMap

    # TODO: Consider a threading.Lock for editing permissions.
    # TODO: Consider asyncio.Lock instances for non-thread-safe state updates during
    #  execution and dispatching.

    def __init__(self, *,
                 loop: asyncio.AbstractEventLoop,
                 executor_factory: ExecutorFactory):
        """
        The event loop for the program should be launched in the root thread,
        preferably early in the application launch.
        Whether the WorkflowManager uses it directly,
        it is useful to require the client to provide the event loop,
        if for no other reason than to ensure that one exists.

        Args:
            loop: event loop, such as from asyncio.new_event_loop()
            executor_factory: Implementation-specific callable to get a run time work
            manager.
        """
        # We are moving towards a composed rather than a derived WorkflowManager Context.
        # Note that we can require the super().__init__() to be called in derived classes,
        # so it is not non-sensical for an abc.ABC to have an __init__ method.
        if not isinstance(loop, asyncio.AbstractEventLoop):
            raise TypeError(
                'Workflow manager requires an event loop object compatible with '
                'asyncio.AbstractEventLoop.')
        if loop.is_closed():
            raise ProtocolError('Event loop does not appear to be ready to use.')
        logger.debug(f'{repr(self)} acquired event loop {repr(loop)} at loop time '
                     f'{loop.time()}.')
        self._asyncio_event_loop = loop

        if not callable(executor_factory):
            raise TypeError('*executor_factory* argument must be a callable.')
        self._executor_factory = executor_factory

        # Basic Context implementation details
        # TODO: Tasks should only writable within a WorkflowEditor context.
        self.tasks = TaskMap()  # Map UIDs to task Futures.

        self._dispatcher: typing.Union[weakref.ref, None] = None
        self._dispatcher_lock = asyncio.Lock()

        self._event_hooks: typing.Mapping[str, typing.MutableSet[AddItemCallback]] = {
            'add_item': set()
        }

    def loop(self):
        return self._asyncio_event_loop

    def subscribe(self, event: str, callback: AddItemCallback):
        """Register a callback for method call events.

        Currently, only the add_item method provides a subscribable event.

        Registered callbacks will be called before the subscribed-to method returns.
        The callback will be given a dictionary with the event name as a key
        and a value that is the same as the argument(s) to the method.

        Unsubscribe with unsubscribe() to avoid unexpected behavior and to avoid
        prolonging the life of the object providing the callback.
        """
        hook: typing.MutableSet[AddItemCallback] = self._event_hooks[event]
        # TODO: Do we need to allow the callback to include a contextvars.Context with
        #  the registration?
        if callback in hook:
            raise ValueError('Provided callback is already registered.')
        hook.add(callback)

    def unsubscribe(self, event: str, callback: AddItemCallback):
        hook = self._event_hooks[event]
        hook.remove(callback)

    def item(self, identifier) -> ItemView:
        """Access an item in the managed workflow.
        """
        # Consider providing the consumer context when acquiring access.
        # Consider limiting the scope of access requested.
        if isinstance(identifier, typing.SupportsBytes):
            identifier = bytes(identifier)
        if not isinstance(identifier, bytes):
            raise MissingImplementationError(
                'Item look-up is currently limited to UID byte sequences.')
        identifier = bytes(identifier)
        logger.debug('Looking for {} in ({})'.format(
            identifier.hex(),
            ', '.join(key.hex() for key in self.tasks.keys())
        ))
        if identifier not in self.tasks:
            raise KeyError(f'WorkflowManager does not have item {identifier}')
        item_view = ItemView(context=self, uid=identifier)

        return item_view

    @contextlib.contextmanager
    def edit_item(self, identifier) -> Task:
        """Scoped workflow item editor.

        Grant the caller full access to the managed task.

        """
        # TODO: Use a proxy object that is easier to inspect and roll back.
        item = self.tasks[identifier]

        # We can add a lot more sophistication here.
        # For instance,
        # * we should lock items for editing
        # * we can update state metadata at each phase of the contextmanager protocol
        # * we can execute hooks based on the state change detected when the client
        #   releases the editor context.

        yield item

    # TODO: Consider helper functionality and `label` support.
    # def find(self, label: str = None):
    #     """Try to locate a workflow object.
    #
    #     Find reference by label. Find owner of non-local resource, if known.
    #     """

    def default_dispatcher(self):
        """Get a default dispatcher instance, if available.

        Provide a hint to scalems.run() on how to execute work in this scope.

        WorkflowManager implementations define their own semantics. If implemented,
        the returned object should be an AsyncContextManager. If the dispatching
        facility is not reentrant, the WorkflowManager may raise ProtocolError.

        WorkflowManagers are not required to provide a default dispatcher.
        """
        return None

    @contextlib.asynccontextmanager
    async def dispatch(self, dispatcher: 'Queuer' = None, params=None):
        """Enter the execution dispatching state.

        Attach to a dispatching executor, then provide a scope for concurrent activity.
        This is also the scope during which the RADICAL Pilot Session exists.

        Provide the executor with any currently-managed work in a queue.
        While the context manager is active, new work added to the queue will be picked up
        by the executor. When the context manager is exited, new work will resume
        queuing locally and the remote tasks will be resolved, then the dispatcher
        will be disconnected.

        Currently, we tie the lifetime of the dispatcher to this context manager.
        When leaving the `with` block, we trigger the executor to clean-up and wait for
        its task to complete.
        We may choose some other relationship in the future.

        Args:
            dispatcher: A queue processor that will subscribe to the add_item hook to
            feed the executor.
            params: a parameters object relevant to the execution back-end

        .. todo:: Clarify re-entrance policy, thread-safety, etcetera, and enforce.

        """

        # 1. Bind a new executor to its queue.
        # 2. Bind a dispatcher to the executor.
        # 3. Enter executor context.
        # 4. Enter dispatcher context.
        #         # 1. (While blocking event loop in UI thread) Install a hook
        #              for the queuer to catch new calls to add_item (the
        #              dispatcher_queue).
        #         # 2. Get snapshot of current workflow state with which to initialize
        #              the executor. (Unblock.)
        #         # 3. Spool workflow snapshot to executor.
        #         # 4. Start dispatcher queue runner.
        #         # 5. Yield.
        # 5. Exit dispatcher context.
        # 6. Exit executor context.
        # TODO: Add lock context for WorkflowManager event hooks
        #  rather than assume the UI and event loop are always in the same thread.

        executor = self._executor_factory(manager=self, params=params)

        # Avoid race conditions while checking for a running dispatcher.
        # TODO: Clarify dispatcher state machine and remove/replace assertions.
        # Warning: The dispatching protocol is immature.
        # Initially, we don't expect contention for the lock,
        # and if there is contention, it probably represents
        # an unintended race condition or systematic dead-lock.
        assert not self._dispatcher_lock.locked()
        async with self._dispatcher_lock:
            # Dispatching state may be reentrant, but it does not make sense to
            # re-enter through this call structure.
            if self._dispatcher is not None:
                raise ProtocolError(
                    f'Already dispatching through {repr(self._dispatcher())}.')
            if dispatcher is None:
                dispatcher = Queuer(source=self,
                                    command_queue=executor.queue(),
                                    dispatcher_lock=self._dispatcher_lock)
                self._dispatcher = dispatcher
            else:
                self._dispatcher = weakref.proxy(dispatcher)

        try:
            # Manage scope of executor operation with a context manager.
            # RP does not yet use an event loop, but we can use async context manager
            # for future compatibility with asyncio management of network connections,
            # etc.
            #
            # Note: the executor owns a rp.Session during operation.
            async with executor as dispatching_session:
                async with dispatcher:
                    # We can surrender control here and leave the executor and
                    # dispatcher tasks active while evaluating a `with` block suite
                    # for the `dispatch` context manager.
                    yield dispatching_session
                # Executor receives a *stop* command in __aexit__.

        except Exception as e:
            logger.exception(
                f'Uncaught exception while in dispatching context: {str(e)}')
            raise e

        finally:
            # Warning: The dispatching protocol is immature.
            # Initially, we don't expect contention for the lock,
            # and if there is contention, it probably represents
            # an unintended race condition or systematic dead-lock.
            # TODO: Clarify dispatcher state machine and remove/replace assertions.
            #       Be on the look-out for nested context managers and usage in
            #       `finally` blocks.
            assert not self._dispatcher_lock.locked()
            async with self._dispatcher_lock:
                self._dispatcher = None

            dispatcher_exception = dispatcher.exception()
            if dispatcher_exception:
                if isinstance(dispatcher_exception, asyncio.CancelledError):
                    logger.info('Dispatching queue processor cancelled.')
                else:
                    assert not isinstance(dispatcher_exception, asyncio.CancelledError)
                    logger.exception('Queuer encountered exception.',
                                     exc_info=dispatcher_exception)
            else:
                if not dispatcher.queue().empty():
                    logger.error(
                        'Queuer finished while items remain in dispatcher queue. '
                        'Approximate size: {}'.format(
                            dispatcher.queue().qsize()))

            executor_exception = executor.exception()
            if executor_exception:
                if isinstance(executor_exception, asyncio.CancelledError):
                    logger.info('Executor cancelled.')
                else:
                    assert not isinstance(executor_exception, asyncio.CancelledError)
                    logger.exception('Executor task finished with exception',
                                     exc_info=executor_exception)
            else:
                if not executor.queue().empty():
                    # TODO: Handle non-empty queue.
                    # There are various reasons that the queue might not be empty and
                    # we should clean up properly instead of bailing out or compounding
                    # exceptions.
                    # TODO: Check for extraneous extra *stop* commands.
                    logger.error(
                        'Bug: Executor left tasks in the queue without raising an '
                        'exception.')

            logger.debug('Exiting {} dispatch context.'.format(type(self).__name__))

    # @abc.abstractmethod
    # def add_task(self, operation: str, bound_input):
    #     """Add a task to the workflow.
    #
    #     Arguments:
    #         operation: named operation to perform
    #         bound_input: reference to a workflow object compatible with operation input.
    #
    #     Returns:
    #         Reference to the new task.
    #
    #     TODO: Resolve operation implementation to dispatch task configuration.
    #     """
    #     ...

    def add_item(self, task_description) -> ItemView:
        # # TODO: Resolve implementation details for *operation*.
        # if operation != 'scalems.executable':
        #     raise MissingImplementationError('No implementation for {} in {}'.format(operation, repr(self)))
        # # Copy a static copy of the input.
        # # TODO: Dispatch tasks addition, allowing negotiation of Context capabilities and subscription
        # #  to resources owned by other Contexts.
        # if not isinstance(bound_input, scalems.subprocess.SubprocessInput):
        #     raise ValueError('Only scalems.subprocess.SubprocessInput objects supported as input.')

        # TODO: Replace with type-based dispatching or some normative interface test.
        from ..subprocess import Subprocess
        if not isinstance(task_description, (Subprocess, dict)):
            raise MissingImplementationError('Operation not supported.')

        if hasattr(task_description, 'uid'):
            uid: bytes = task_description.uid()
            if uid in self.tasks:
                # TODO: Consider decreasing error level to `warning`.
                raise DuplicateKeyError('Task already present in workflow.')
            logger.debug('Adding {} to {}'.format(str(task_description), str(self)))
            record = {
                'uid': uid.hex(),
                'type': task_description.resource_type().scoped_identifier(),
                'input': {}
            }
            task_input = task_description.input_collection()
            for field in dataclasses.fields(task_input):
                name = field.name
                try:
                    # TODO: Need serialization typing.
                    record['input'][name] = getattr(task_input, name)
                except AttributeError as e:
                    raise InternalError('Unexpected missing field.') from e
        else:
            assert isinstance(task_description, dict)
            assert 'uid' in task_description
            uid = task_description['uid']
            implementation_identifier = task_description.get('implementation', None)
            if not isinstance(implementation_identifier, list):
                raise DispatchError('Bug: bad schema checking?')

            if uid in self.tasks:
                # TODO: Consider decreasing error level to `warning`.
                raise DuplicateKeyError('Task already present in workflow.')
            logger.debug('Adding {} to {}'.format(str(task_description), str(self)))
            record = {
                'uid': uid.hex(),
                'type': tuple(implementation_identifier),
                'input': task_description
            }
        serialized_record = json.dumps(record, default=encode)

        # TODO: Make sure there are no artifacts of shallow copies that may result in
        #       a user modifying nested objects unexpectedly.
        item = Task(self, serialized_record)
        # TODO: Check for ability to dispatch.
        #  Note module dependencies, etc. and check in target execution environment
        #  (e.g. https://docs.python.org/3/library/importlib.html#checking-if-a-module-can-be-imported)

        # TODO: Provide a data descriptor and possibly a more formal Workflow class.
        # We do not yet check that the derived classes actually initialize self.tasks.
        self.tasks[uid] = item

        task_view = ItemView(context=self, uid=uid)

        # TODO: Register task factory (dependent on executor).
        # TODO: Register input factory (dependent on dispatcher and task factory /
        #  executor).
        # TODO: Register results handler (dependent on dispatcher end points).

        # TODO: Consider an abstract event hook for `add_item` and other (decorated)
        #  methods.
        # Internal functionality can probably explicitly register and unregister,
        # accounting for the current details of thread safety.
        # External access will need to be in terms of a concurrency framework,
        # so we can use a scoped `async with event_subscription`
        # to create an asynchronous iterator that a coroutine can use to receive
        # add_item messages
        # (with some means to externally end the subscription,
        # either through the generator protocol directly or through logic in the
        # provider of the iterator)
        for callback in self._event_hooks['add_item']:
            # TODO: Do we need to provide a contextvars.Context object to the callback?
            logger.debug(f'Running dispatching hook for add_item subscriber '
                         f'{repr(callback)}.')
            callback(_CommandQueueAddItem({'add_item': uid}))

        return task_view


class Queuer:
    """Maintain the active dispatching state for a managed workflow.

    The Queuer, WorkflowManager, and Executor lifetimes do not need to be
    coupled, but their periods of activity should be synchronized in certain ways
    (aided using the Python context manager protocol).

    The Queuer must have access to an active Executor while the Queuer
    is active.

    When entering the Queuer context manager, a task is created to transfer items
    from the dispatch queue to the execution queue. The task is allowed to drain the
    queue before the context manager yields to the ``with`` block. The task will
    continue to process the queue asynchronously if new items appear.

    When exiting the Queuer context manager, the items currently in the
    dispatching queue are processed and the the dispatcher task is finalized.

    The dispatcher queue is a queue.SimpleQueue so that WorkflowManager.add_item()
    can easily use a concurrency-safe callback to add items whether or not the
    dispatcher is active.
    """

    command_queue: asyncio.Queue
    """Target queue for dispatched commands."""

    source_context: WorkflowManager
    """Owner of the workflow items being queued."""

    _dispatcher_lock: asyncio.Lock
    """Provided by caller to allow safe transitions of dispatching state."""

    _queue_runner_task: asyncio.Task
    """Queue, owned by this object, of Workflow Items being processed for dispatch."""

    def __init__(self,
                 source: WorkflowManager,
                 command_queue: asyncio.Queue,
                 dispatcher_lock=None):
        """Create a queue-based workflow dispatcher.

        Initialization and deinitialization occurs through
        the Python (async) context manager protocol.
        """

        self.source_context = source
        self._dispatcher_queue = _queue.SimpleQueue()
        self.command_queue = command_queue

        if not isinstance(dispatcher_lock, asyncio.Lock):
            raise TypeError('An asyncio.Lock is required to control dispatcher state.')
        self._dispatcher_lock = dispatcher_lock

        self._exception = None

    def queue(self):
        return self._dispatcher_queue

    def put(self, item: typing.Union[_CommandQueueAddItem, _CommandQueueControlItem]):
        assert len(item) == 1
        key = list(item.keys())[0]
        if key not in {'command', 'add_item'}:
            raise APIError('Unrecognized queue item representation.')
        self._dispatcher_queue.put(item)

    async def __aenter__(self):
        try:
            # Get a lock while the state is changing.
            # Warning: The dispatching protocol is immature.
            # Initially, we don't expect contention for the lock,
            # and if there is contention, it probably represents
            # an unintended race condition or systematic dead-lock.
            # TODO: Clarify dispatcher state machine and remove/replace assertions.
            assert not self._dispatcher_lock.locked()
            async with self._dispatcher_lock:
                if _dispatcher.get(None):
                    raise APIError(
                        'There is already an active dispatcher in this Context.')
                _dispatcher.set(self)
                # Launch queue processor (proxy executor).
                runner_started = asyncio.Event()
                runner_task = asyncio.create_task(self._queue_runner(runner_started))
                await runner_started.wait()
                self._queue_runner_task = runner_task

                # Without yielding,
                # 1. Install a hook for the queuer to catch new calls to add_item.
                # 2. Get snapshot of current workflow state with which to initialize
                #    the executor.
                # Dont' forget to unsubscribe later!
                # self.source_context.subscribe('add_item', self._dispatcher_queue.put)
                self.source_context.subscribe('add_item', self.put)
                # TODO: Topologically sort DAG!
                initial_task_list = list(self.source_context.tasks.keys())
                try:
                    for _task_id in initial_task_list:
                        self.command_queue.put_nowait(QueueItem({'add_item': _task_id}))
                except asyncio.QueueFull as e:
                    raise DispatchError('Executor was unable to receive initial '
                                        'commands.') from e
                # It is now safe to yield.

                # TODO: Add lock context for WorkflowManager event hooks
                #  rather than assume the UI and event loop are always in the same thread.

            return self
        except Exception as e:
            self._exception = e
            raise e

    async def _single_iteration_queue(self,
                                      source: _queue.SimpleQueue,
                                      target: asyncio.Queue):
        """Transfer one queue item.

        If a *stop* command is encountered, self-cancel after transfering command.

        To avoid race conditions while stopping queue processing,
        place a *stop* command in *source* and asyncio.shield() a call
        to this coroutine in a *try: ... except: ...* block.

        Note that the caller will then receive CancelledError after *stop* command has
        been transferred.

        Raises:
            queue.Empty if *source* is empty
            asyncio.CancelledError when cancelled or *stop* is received.

        """
        command: QueueItem = source.get_nowait()
        logger.debug(f'Processing command {repr(command)}')

        await target.put(command)

        # TODO: Use formal RPC protocol.
        if 'control' in command:
            # Note that we don't necessarily need to stop managing the dispatcher queue
            # at this point, but the Executor will be directed to shut down,
            # so we must not put anything else onto the command queue until we have a
            # new command queue or a new executor.
            if command['control'] == 'stop':
                raise asyncio.CancelledError()
            else:
                raise ProtocolError('Unknown command: {}'.format(command['control']))
        else:
            if 'add_item' not in command:
                # TODO: We might want a call-back or Event to force errors before the
                #  queue-runner task is awaited.
                raise MissingImplementationError(
                    f'Executor has no implementation for {str(command)}'
                )
        return command

    async def _queue_runner(self, processing_state: asyncio.Event):
        processing_state.set()
        while True:
            try:
                await asyncio.shield(
                    self._single_iteration_queue(source=self._dispatcher_queue,
                                                 target=self.command_queue))
            except _queue.Empty:
                # Wait a moment and try again.
                await asyncio.sleep(0.5)

    async def _drain_queue(self):
        """Wait until the dispatcher queue is empty, then return.

        Use in place of join() for event-loop treatment of *queue.SimpleQueue*.
        """
        while not self._dispatcher_queue.empty():
            await asyncio.sleep(0.1)

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: C901
        """Clean up at context exit.

        Drain the dispatching queue and exit.

        Unsubscribes from the WorkflowManager add_item hook, deactivates the
        dispatching context, and exits.
        Does not cancel or send instructions to the Executor managing the command queue.
        """
        # Note that this coroutine could take a long time and could be cancelled at
        # several points.
        cancelled_error = None
        # The dispatching protocol is immature. Initially, we don't expect contention for the lock, and if there is
        # contention, it probably represents an unintended race condition or systematic dead-lock.
        # TODO: Clarify dispatcher state machine and remove/replace assertions.
        assert not self._dispatcher_lock.locked()
        async with self._dispatcher_lock:
            try:
                self.source_context.unsubscribe('add_item', self.put)
                _dispatcher.set(None)

                # Stop the dispatcher.
                logger.debug('Stopping the SCALEMS RP dispatching queue runner.')

                # Wait for the queue to drain or the queue runner to exit or fail.
                drain = asyncio.create_task(self._drain_queue())
                done, pending = await asyncio.wait({drain, self._queue_runner_task},
                                                   return_when=asyncio.FIRST_COMPLETED)
                assert len(done) > 0
                if self._queue_runner_task not in done:
                    if drain in done:
                        self._queue_runner_task.cancel()
                done, _ = await asyncio.wait({self._queue_runner_task})
                assert self._queue_runner_task in done
                if not self._queue_runner_task.cancelled():
                    exception = self._queue_runner_task.exception()
                else:
                    exception = None
                if exception:
                    logger.exception('Queuer queue processing encountered exception',
                                     exc_info=exception)
                    if self._exception:
                        logger.error('Queuer is already holding an exception.')
                    else:
                        self._exception = exception

            except asyncio.CancelledError as e:
                logger.debug('Queuer context manager received cancellation while '
                             'exiting.')
                cancelled_error = e
            except Exception as e:
                logger.exception('Exception while stopping dispatcher.', exc_info=e)
                if self._exception:
                    logger.error('Queuer is already holding an exception.')
                else:
                    self._exception = e
            finally:
                # Should we do any other clean-up here?
                ...
        if cancelled_error:
            raise cancelled_error

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False

    def exception(self) -> typing.Union[None, Exception]:
        return self._exception

    # We don't currently have a use for a stand-alone Task.
    # We use the async context manager and the exception() method.
    # def __await__(self):
    #     """Implement the asyncio task represented by this object."""
    #     # Note that this is not a native coroutine object; we cannot `await`
    #     # The role of object.__await__() is to return an iterator that will be
    #     # run to exhaustion in the context of an event loop.
    #     # We assume that most of the asyncio activity happens through the
    #     # async context mananager behavior and other async member functions.
    #     # If we choose to `await instance` at all, we need a light-weight
    #     # iteration we can perform to surrender control of the event loop,
    #     # and then just do some sort of tidying or reporting that doesn't fit well
    #     # into __aexit__(), such as the ability to return a value.
    #
    #     # # Note: We can't do this without first wait on some sort of Done event...
    #     # failures = []
    #     # for t in self.rp_tasks:
    #     #     logger.info('%s  %-10s : %s' % (t.uid, t.state, t.stdout))
    #     #     if t.state != rp.states.DONE or t.exit_code != 0:
    #     #         logger.error(f'RP Task unsuccessful: {repr(t)}')
    #     #         failures.append(t)
    #     # if len(failures) > 0:
    #     #     warnings.warn('Unsuccessful tasks: ' + ', '.join([repr(t) for t in failures]))
    #
    #     yield
    #     if self._exception:
    #         raise self._exception
    #
    #     # # If we want to provide a "Future-like" interface, we should support the callback
    #     # # protocols and implement the following generator function.
    #     # if not self.done():
    #     #     self._asyncio_future_blocking = True
    #     #     # ref https://docs.python.org/3/library/asyncio-future.html#asyncio.isfuture
    #     #
    #     #     yield self  # This tells Task to wait for completion.
    #     # if not self.done():
    #     #     raise RuntimeError("The dispatcher task was not 'await'ed.")
    #     # Ref PEP-0380: "return expr in a generator causes StopIteration(expr)
    #     # to be raised upon exit from the generator."
    #     # The Task works like a `result = yield from awaitable` expression.
    #     # The iterator (generator) yields until exhausted,
    #     # then raises StopIteration with the value returned in by the generator function.
    #     # return self.result()  # May raise too.
    #     # # Otherwise, the only allowed value from the iterator is None.


class Scope(typing.NamedTuple):
    """Backward-linked list (potentially branching) to track nested context.

    There is not much utility to tracking the parent except for introspection
    during debugging. The previous state is more appropriately held within the
    closure of the context manager. This structure may be simplified without warning.
    """
    parent: typing.Union[None, WorkflowManager]
    current: WorkflowManager


# If we require an event loop to be provided to the WorkflowManager, then
# we should not instantiate a default context on module import. We don't really
# want to hold an event loop object at module scope, and we want to give as much
# opportunity as possible for the caller to provide an event loop.
# # Root workflow context for the interpreter process.
# _interpreter_context = DefaultContext()

# Note: Scope indicates the hierarchy of "active" WorkflowManager instances
# (related by dispatching).
# This is separate from WorkflowManager lifetime and ownership.
# WorkflowManagers should track their own activation status and provide logic for
# whether to allow reentrant dispatching.
# TODO: Shouldn't the previous "current" be notified or negotiated with? Should we be
#  locking something?
# Note that it makes no sense to start a dispatching session without concurrency,
# so we can think in terms of a parent context doing contextvars.copy_context().run(...)
# I think we have to make sure not to nest scopes without a combination of copy_context
# and context managers, so we don't need to track the parent scope. We should also be
# able to use weakrefs.
current_scope = contextvars.ContextVar('current_scope')


def get_context():
    """Get a reference to the manager of the current workflow scope."""
    # TODO: Redocument and adjust semantics.
    # The contextvars and get_context should only be used in conjunction with
    # a workflow_scope() context manager that is explicitly not thread-safe, but
    # which can employ some checks for non-multi-threading access assumptions.
    # get_context() is used to determine the default workflow manager when *context*
    # is not provided to scalems object factories, scalems.run(), scalems.wait() and
    # (non-async) `result()` methods. Default *context* values are a user convenience
    # and so should only occur in the root thread for the UI / high-level scripting
    # interface.
    # Async coroutines can safely use get_context(), but should not use the
    # non-async workflow_scope() context manager for nested scopes without wrapping
    # in a contextvars.run().
    try:
        _scope: Scope = current_scope.get()
        current_context = _scope.current
        logger.debug(f'Scope queried with get_context() {repr(current_context)}')
        # This check is in case we use weakref.ref:
        if current_context is None:
            raise ProtocolError('Context for current scope seems to have disappeared.')
    except LookupError:
        logger.debug('Scope was queried, but has not yet been set.')
        current_context = None
    return current_context


@contextlib.contextmanager
def scope(context):
    """Set the current workflow management within a clear scope.

    Restore the previous workflow management scope on exiting the context manager.

    Within the context managed by *scope*, get_context() will return *context*.

    Not thread-safe. In general, this context manage should only be used in the
    root thread.
    """
    parent = get_context()
    dispatcher = _dispatcher.get(None)
    if dispatcher is not None and parent is not dispatcher:
        raise ProtocolError(
            'It is unsafe to use concurrent scope() context managers in an asynchronous '
            'context.')
    logger.debug('Entering scope of {}'.format(str(context)))
    current = context
    token = current_scope.set(
        Scope(
            parent=parent,
            current=current)
    )
    if token.var.get().parent is current:
        logger.warning('Unexpected re-entrance. Workflow is already managed by '
                       f'{repr(current)}')
    if token.old_value is not token.MISSING and token.old_value.current != \
            token.var.get().parent:
        raise ProtocolError(
            'Unrecoverable race condition: multiple threads are updating global context '
            'unsafely.')
    # Try to confirm that current_scope is not already subject to modification by another
    #  context manager in a shared asynchronous context.
    # This nesting has to have LIFO semantics both in and out of coroutines,
    # and cannot block.
    # One option would be to refuse to nest if the current scope is not the root scope and
    # the root scope has an active dispatcher. Note that a dispatcher should use
    # contextvars.copy_context().run() and set a new root context.
    # Alternatively, we could try to make sure that no asynchronous yields are allowed
    # when the current context is a nested scope within a dispatcher context,
    # but technically this is okay as long as a second scope is not nested within the
    # first from within a coroutine that might not finish until after the first scope
    # finishes.
    try:
        yield current
    finally:
        """Exit context manager without processing exceptions."""
        logger.debug('Leaving scope of {}'.format(str(context)))
        # Restore context module state since we are not using contextvars.Context.run()
        # or equivalent.
        if token.var.get().parent is not parent or token.var.get().current is not current:
            raise ProtocolError(
                'Unexpected re-entrance. Workflow scope changed while in context '
                f'manager {repr(current)}.')
        else:
            token.var.reset(token)
        # TODO: Consider if/how we should process un-awaited tasks.


async def manage_execution(executor: RuntimeManager,
                           *,
                           processing_state: asyncio.Event):
    """Process workflow messages until a stop message is received.

    Initial implementation processes commands serially without regard for possible
    concurrency.

    Towards concurrency:
        We can create all tasks without awaiting any of them.

        Some tasks will be awaiting results from other tasks.

        All tasks will be awaiting a asyncio.Lock or asyncio.Condition for each
        required resource, but must do so indirectly.

        To avoid dead-locks, we can't have a Lock object for each resource unless
        they are managed by an intermediary that can do some serialization of requests.
        In other words, we need a Scheduler that tracks the resource pool, packages
        resource locks only when they can all be acquired without race conditions or blocking,
        and which then notifies the Condition for each task that it is allowed to run.

        It should not do so until the dependencies of the task are known to have
        all of the resources they need to complete (running with any dynamic dependencies
        also running) and, preferably, complete.

        Alternatively, the Scheduler can operate in blocks, allocating all resources,
        offering the locks to tasks, waiting for all resources to be released, then repeating.
        We can allow some conditions to "wake up" the scheduler to back fill a block
        of resources, but we should be careful with that.

        (We still need to consider dynamic tasks that
        generate other tasks. I think the only way to distinguish tasks which can't be
        dynamic from those which might be would be with the `def` versus `async def` in
        the implementing function declaration. If we abstract `await` with `scalems.wait`,
        we can throw an exception at execution time after checking a ContextVar.
        It may be better to just let implementers use `await` for dynamically created tasks,
        but we need to make the same check if a function calls `.result()` or otherwise
        tries to create a dependency on an item that was not allocated resources before
        the function started executing. In a conservative first draft, we can simply
        throw an exception if a non-`async def` function attempts to call a scalems workflow
        command like add_item while in an executing context.)

    """
    queue = executor.queue()
    updater = executor.updater()

    # Acknowledge that the coroutine is running and will immediately begin processing
    # queue items.
    processing_state.set()
    # Note that if an exception is raised here, the queue will never be processed.

    # Could also accept a "stop" Event object for the loop conditional,
    # but we would still need a way to yield on an empty queue until either
    # the "stop" event or an item arrives, and then we might have to account
    # for queues that potentially never yield any items, such as by sleeping briefly.
    # We should be able to do
    #     signal_task = asyncio.create_task(stop_event.wait())
    #     queue_getter = asyncio.create_task(command_queue.get())
    #     waiter = asyncio.create_task(asyncio.wait((signal_task, queue_getter), return_when=FIRST_COMPLETED))
    #     while await waiter:
    #         done, pending = waiter.result()
    #         assert len(done) == 1
    #         if signal_task in done:
    #             break
    #         else:
    #             command: QueueItem = queue_getter.result()
    #         ...
    #         queue_getter = asyncio.create_task(command_queue.get())
    #         waiter = asyncio.create_task(asyncio(wait((signal_task, queue_getter), return_when=FIRST_COMPLETED))
    #
    while True:
        # Note: If the function exits at this line, the queue may be missing a call
        #  to task_done() and should not be `join()`ed. It does not seem completely
        #  clear whether the `get()` will have completed if the task for this function
        #  is canceled or a Task.send() or Task.throw() is used. Alternatively, we could
        #  include the `get()` in the `try` block and catch a possible ValueError at the
        #  task_done() call, in case we arrive there without Queue.get() having completed.
        #  However, that could allow a Queue.join() to complete early by accident.
        command: QueueItem = await queue.get()
        # Developer note: The preceding line and the following try/finally block are coupled!
        # Once we have awaited asyncio.Queue.get(), we _must_ have a corresponding
        # asyncio.Queue.task_done(). For tidiness, we immediately enter a `try` block with a
        # `finally` suite. Don't separate these without considering the Queue protocol.

        try:
            if not len(command.items()) == 1:
                raise ProtocolError('Expected a single key-value pair.')
            logger.debug(f'Processing command {repr(command)}')

            # TODO: Use formal RPC protocol.
            if 'control' in command:
                if command['control'] == 'stop':
                    logger.debug('Execution manager received stop command.')
                    # This effectively breaks the `while True` loop, but may not be obvious.
                    # Consider explicit `break` to clarify that we want to run off the end
                    # of the function.
                    return
                else:
                    raise ProtocolError('Unknown command: {}'.format(command['control']))
            if 'add_item' not in command:
                # This will end queue processing. Tasks already submitted may still
                # complete. Tasks subsequently added will update the static part of
                # the workflow (WorkflowManager) and be eligible for dispatching in
                # another dispatching session.
                # If we need to begin to address the remaining queued items before
                # this (the queue processor) task is awaited, we could insert a
                # Condition here to alert some sort of previously scheduled special
                # tear-down task.
                raise MissingImplementationError('Executor has no implementation for {}'.format(str(command)))

            key = command['add_item']
            with executor.source_context.edit_item(key) as item:
                if not isinstance(item, Task):
                    raise InternalError(
                        'Bug: Expected {}.edit_item() to return a _context.Task'.format(repr(executor.source_context)))

                # Bind the WorkflowManager item to an RP Task.
                task = await updater.submit(item=item)

                if task.done():
                    # Stop processing the queue if task was cancelled or errored.
                    # TODO: Test this logical branch.
                    if task.cancelled():
                        logger.info('Stopping queue processing after unexpected '
                                    f'cancellation of task {task}')
                        return
                    elif task.exception():
                        logger.error(f'Task failed: {task}')
                        raise task.exception()
                    else:
                        logger.debug(f'Task {task} already done. Continuing.')
                else:
                    executor.submitted_tasks.append(task)

        except Exception as e:
            logger.debug('Leaving queue runner due to exception.')
            raise e
        finally:
            # Warning: there is a tiny chance that we could receive a asyncio.CancelledError at this line
            # and fail to decrement the queue.
            logger.debug('Releasing "{}" from command queue.'.format(str(command)))
            queue.task_done()
