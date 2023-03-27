"""WorkflowManager and Item interfaces.

Information and resources supporting a defined workflow are managed in the scope
of a :py:class:`~scalems.workflow.WorkflowManager`.
All ScaleMS API calls and references occur in or between the scopes of `WorkflowManager` instances.
When the ScaleMS API is active, there is a notion of a "current" WorkflowContext.

When the scalems module is simply imported, the default context is able to
discover local workflow state from the working directory and manage a workflow
representation, but it is not able to dispatch the work directly for execution.

SCALE-MS optimizes data flow and data locality in part by attributing all
workflow references to well-defined scopes. Stateful API facilities, workflow
state, and scoped references are managed as WorkflowManager instances.

"Workflow Manager" is an important parameter in several cases.
Execution management tools like scalems.run(), scalems.wait(), and scalems.dispatch()
update the workflow managed in a particular scope, possibly by interacting with
other scopes. Commands for declaring work or data add items to specific instances
of workflow managers. Internally, SCALEMS explicitly refers to theses scopes as
*manager*, or *context*, depending on the nature of the code collaboration.
*manager* may appear as an optional parameter for user-facing functions to allow
a particular managed workflow to be specified.

This module supports `scope()` and `get_scope()` with internal module state.
These tools interact with the context management of the asynchronous dispatching,
but note that they are not thread-safe. scope() should not be used in
a coroutine except in the root coroutine of a Task or otherwise within the scope
of a contextvars.copy_context().run(). scalems will try to flag misuse by raising
a ProtocolError, but please be sensible.
"""

__all__ = ("WorkflowManager", "get_scope", "scope", "wait", "workflow_item_director_factory")

import asyncio
import contextlib
import contextvars
import dataclasses
import functools
import json
import logging
import os
import queue as _queue
import threading
import typing
import weakref

from scalems import exceptions as _exceptions

from scalems.store import FileStoreManager
from scalems.messages import CommandQueueAddItem
from scalems.messages import CommandQueueControlItem
from scalems.messages import QueueItem
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import DuplicateKeyError
from scalems.exceptions import InternalError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.exceptions import ScaleMSError
from scalems.exceptions import ScopeError
from scalems.identifiers import TypeIdentifier
from scalems.serialization import encode
from scalems.unique import next_monotonic_integer

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


class Description:
    def shape(self) -> tuple:
        return self._shape

    def type(self) -> TypeIdentifier:
        return self._type

    def __init__(self, resource_type: TypeIdentifier, *, shape: tuple = (1,)):
        type_id = TypeIdentifier.copy_from(resource_type)
        if type_id is None:
            raise ValueError(f"Could not create a TypeIdentifier for {repr(resource_type)}")
        # TODO: Further input validation
        self._shape = shape
        self._type = type_id


class ItemView:
    """Standard object returned by a WorkflowContext when adding details to the workflow.

    Provides the normative interface for workflow items as a user-space object
    that proxies access through a workflow manager.

    Provides a Future like interface.

    At least in the initial implementation, a ItemView does not extend the lifetime
    of the WorkflowManager to which it refers.
    If the WorkflowManager from which the ItemView was obtained goes out of scope or
    otherwise becomes invalid, some ItemView interfaces can raise exceptions.

    .. todo:: Allows proxied access to future results through attribute access.

    """

    _workflow_manager: weakref.ReferenceType["WorkflowManager"]

    def uid(self) -> bytes:
        """Get the canonical unique identifier for this task.

        The identifier is universally unique and can be used to query any
        workflow manager for awareness of the task and (if the context is aware
        of the task) to get a view of the task.

        Returns:
            256-bit binary digest as a 32 element byte sequence.
        """
        return bytes(self._uid)

    def manager(self) -> "WorkflowManager":
        manager = self._workflow_manager()
        if manager is None:
            raise ScopeError("Out of scope. Managing context no longer exists!")
        else:
            if not isinstance(manager, WorkflowManager):
                raise APIError("Bug: ItemView._workflow_manager must weakly reference a WorkflowManager.")
        return manager

    def done(self) -> bool:
        """Check the status of the task.

        Returns:
            true if the task has finished.

        """
        manager = self.manager()
        return manager.tasks[self.uid()].done()

    def result(self):
        """Get a local object of the tasks's result type.

        .. todo:: Forces dependency resolution.

        """
        manager = self.manager()
        # TODO: What is the public interface to the tasks or completion status?
        # Note: We want to keep the View object as lightweight as possible, such
        # as storing just the weak ref to the manager, and the item identifier.
        return manager.tasks[self.uid()].result()

    def description(self) -> Description:
        """Get a description of the resource type."""
        manager = self.manager()
        return manager.tasks[self.uid()].description()

    def __getattr__(self, item):
        """Proxy attribute accessor for special task members.

        If the workflow element provides the requested interface, the managing
        Context will provide appropriate access. For "result" attributes, the
        returned reference supports the Future interface.
        """
        # We don't actually want to do this check here, but this is essentially what
        # needs to happen:
        #     assert hasattr(self.description().type().result_description().type(), item)
        manager: WorkflowManager = self.manager()
        task = manager.tasks[self.uid()]  # type: Task
        try:
            return getattr(task, item)
        except KeyError as e:
            raise e

    def __init__(self, manager, uid: bytes):
        self._workflow_manager = weakref.ref(manager)
        if isinstance(uid, bytes) and len(uid) == 32:
            self._uid = uid
        else:
            raise ProtocolError(f"uid should be a 32-byte binary digest (bytes). Got {repr(uid)}")


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
            raise InvalidStateError("Called result() on a Task that is not done.")
        return self._result

    def description(self) -> Description:
        decoded_record = json.loads(self._serialized_record)
        resource_type = TypeIdentifier(tuple(decoded_record["type"]))
        # TODO: Handle multidimensional references.
        shape = (1,)
        value = Description(resource_type=resource_type, shape=shape)
        return value

    def uid(self) -> bytes:
        if not isinstance(self._uid, bytes):
            raise InternalError("Task._uid was stored as bytes. Implementation changed?")
        return bytes(self._uid)

    def done(self) -> bool:
        return self._done.is_set()

    def __getattr__(self, item):
        # TODO: Manage internal state updates.
        # TODO: Respect different attribute type semantics.
        try:
            decoded_record = json.loads(self._serialized_record)
            logger.debug("Decoded {}: {}".format(type(decoded_record).__name__, str(decoded_record)))
            value = decoded_record[item]
        except json.JSONDecodeError as e:
            raise AttributeError('Problem retrieving "{}"'.format(item)) from e
        except KeyError as e:
            logger.debug('Did not find "{}" in {}'.format(item, self._serialized_record))
            raise AttributeError('Problem retrieving "{}"'.format(item)) from e
        return value

    def __init__(self, manager: "WorkflowManager", record: str):
        self._serialized_record = str(record)
        decoded_record = json.loads(self._serialized_record)

        self._uid = bytes.fromhex(decoded_record["uid"])
        if not len(self._uid) == 256 // 8:
            raise ProtocolError("UID is supposed to be a 256-bit hash digest. " f"Got {repr(self._uid)}")
        self._done = asyncio.Event()
        self._result = None

        # As long as we are storing Tasks in the workflow, we cannot store
        # workflows in Tasks.
        self._context = weakref.ref(manager)

    def set_result(self, result):
        # Not thread-safe.
        if self._done.is_set():
            raise ProtocolError("Result is already set for {}.".format(repr(self)))
        self._result = result
        self._done.set()
        logger.debug("Result set for {} in {}".format(self.uid().hex(), str(self._context())))

    def manager(self):
        return self._context()

    # @classmethod
    # def deserialize(cls, context, record: str):
    #     item_view = context.add_item(record)
    #     return item_view

    def serialize(self):
        return self._serialized_record


class TaskMap(dict, typing.MutableMapping[bytes, Task]):
    def __setitem__(self, k: bytes, v: Task) -> None:
        if not isinstance(k, bytes) and isinstance(k, typing.SupportsBytes):
            k = bytes(k)
        if not isinstance(k, bytes) or not isinstance(v, Task):
            raise TypeError("TaskMap maps *bytes* keys to *Task* values.")
        super().__setitem__(k, v)


_QItem_T = typing.TypeVar("_QItem_T", bound=QueueItem)
AddItemCallback = typing.Callable[[_QItem_T], None]


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

    .. todo:: Enforce centralization of Context instantiation for the interpreter process.

    For instance,

    * Implement a root context singleton and require acquisition of new Context
      handles through methods in this module.
    * Use abstract base class machinery to register Context implementations.
    * Require Context instances to track their parent Context, or otherwise
      participate in a single tree structure.
    * Prevent instantiation of Command references without a reference to a Context
      instance.

    TODO:
        In addition to adding callbacks to futures, allow subscriptions to workflow
        updates.
        This allows intermediate updates to be propagated and could be a superset of
        the additem hook.

    Notes:

        workflow manager maintains the workflow state without expensive or stateful
        volatile resources, and can mediate updates to the managed workflow at any
        time. Items enter the graph in an IDLE state. The WorkflowManager can provide
        Futures for the results of the managed items. For IDLE items,
        the WorkflowManager retains a weakref to the issued Futures, which it can use
        to make sure that there is only zero or one Future for a particular result.

        WorkflowManager collaborates with Queuer to transition the graph to an "active"
        or "executing" state. This transition is mediated through the dispatcher_lock.

        Queuer sequences and queues workflow items to be handled, pushing them to a
        dispatch_queue. No state change to the workflow item seems necessary at this
        time.

        The dispatch_queue is read by an ExecutionManager. Items may be processed
        immediately or staged in a command_queue. Workflow items are then either
        SUBMITTED or BLOCKED (awaiting dependencies). Optionally, Items may be marked
        ELIGIBLE and re-queued for batch submission.

        If the ExecutionManager is able to submit a task, the Task has a call-back
        registered for the workflow item. The WorkflowManager needs to convert any
        Future weakrefs to strong references when items are SUBMITTED, and the workflow
        Futures are subscribed to the item. Tasks are wrapped in a scalems object that
        the WorkflowManager is able to take ownership of. BLOCKED items are wrapped in
        Tasks which are subscribed to their dependencies (WorkflowItems should already
        be subscribed to WorkflowItem Futures for any dependencies) and stored by the
        ExecutionManager. When the call-backs for all of the dependencies indicate the
        Item should be processed into an upcoming workload, the Item becomes ELIGIBLE,
        and its wrapper Task (in collaboration with the ExecutionManager) puts it in
        the command_queue.

        As an optimization, and to support co-scheduling, a WorkflowItem call-back can
        provide notification of state changes. For instance, a BLOCKED item may become
        ELIGIBLE once all of its dependencies are SUBMITTED, when the actual Executor
        has some degree of data flow management capabilities.

    """

    tasks: TaskMap
    _executor_factory: typing.Callable
    """Factory for a RuntimeManager or subclass.

    TODO: Resolve circular reference between `execution` and `workflow` modules.
    """

    # TODO: Consider a threading.Lock for editing permissions.
    # TODO: Consider asyncio.Lock instances for non-thread-safe state updates during
    #  execution and dispatching.

    def __init__(
        self, *, loop: asyncio.AbstractEventLoop, executor_factory, directory: typing.Union[str, os.PathLike] = None
    ):
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
            directory: Filesystem path for the workflow file store.
        """
        # We are moving towards a composed rather than a derived WorkflowManager Context.
        # Note that we can require the super().__init__() to be called in derived classes,
        # so it is not non-sensical for an abc.ABC to have an __init__ method.
        if not isinstance(loop, asyncio.AbstractEventLoop):
            raise TypeError("Workflow manager requires an event loop object compatible with asyncio.AbstractEventLoop.")
        if loop.is_closed():
            raise ProtocolError("Event loop does not appear to be ready to use.")
        logger.debug(f"{repr(self)} acquired event loop {repr(loop)} at loop time {loop.time()}.")
        self._asyncio_event_loop = loop

        if not callable(executor_factory):
            raise TypeError("*executor_factory* argument must be a callable.")
        # TODO: Resolve circular reference between `execution` and `workflow` modules.
        # self._executor_factory: ExecutorFactory = executor_factory
        self._executor_factory = executor_factory

        # Basic Context implementation details
        # TODO: Tasks should only writable within a WorkflowEditor context.
        self.tasks = TaskMap()  # Map UIDs to task Futures.

        if directory is None:
            directory = os.getcwd()
        try:
            self._filestoremanager = FileStoreManager(directory=directory)
        except Exception as e:
            logger.exception(f"Could not initialize FileStoreManager for {directory}.", exc_info=e)
            raise ValueError("Need a usable local workflow *directory*.") from e
        # TODO: Restore workflow state from filestore.

        self._dispatcher: typing.Optional[Queuer] = None
        self._dispatcher_lock = asyncio.Lock()

        self._event_hooks: typing.Mapping[str, typing.MutableSet[AddItemCallback]] = {"add_item": set()}

    @property
    def closed(self):
        return self._filestoremanager is None

    def close(self):
        """Finalize and release resources.

        While not usually necessary, an explicit call to close() can force the backing
        data store to be closed and allow exceptions to be caught at a clear point.

        Once close() is called, additional attempts to access the managed workflow may
        raise ScopeError.
        """
        if self._dispatcher is not None:
            raise APIError("Cannot close a WorkflowManager that is actively dispatching work.")
        if self._filestoremanager is not None:
            self._filestoremanager.close()
            self._filestoremanager = None

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
            raise ValueError("Provided callback is already registered.")
        hook.add(callback)

    def unsubscribe(self, event: str, callback: AddItemCallback):
        hook = self._event_hooks[event]
        hook.remove(callback)

    def item(self, identifier) -> ItemView:
        """Access an item in the managed workflow."""
        # Consider providing the consumer context when acquiring access.
        # Consider limiting the scope of access requested.
        if isinstance(identifier, typing.SupportsBytes):
            identifier = bytes(identifier)
        if not isinstance(identifier, bytes):
            raise MissingImplementationError("Item look-up is currently limited to UID byte sequences.")
        identifier = bytes(identifier)
        logger.debug(
            "Looking for {} in ({})".format(identifier.hex(), ", ".join(key.hex() for key in self.tasks.keys()))
        )
        if identifier not in self.tasks:
            raise KeyError(f"WorkflowManager does not have item {str(identifier)}")
        item_view = ItemView(manager=self, uid=identifier)

        return item_view

    @contextlib.contextmanager
    def edit_item(self, identifier) -> typing.Generator[Task, None, None]:
        """Scoped workflow item editor.

        Grant the caller full access to the managed task.

        """
        if self.closed:
            raise ScopeError("WorkflowManager is closed.")
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
    async def dispatch(self, dispatcher: "Queuer" = None, params=None):
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
        if self.closed:
            raise ScopeError("WorkflowManager is closed.")

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
                raise ProtocolError(f"Already dispatching through {repr(self._dispatcher)}.")
            if dispatcher is None:
                dispatcher = Queuer(source=self, command_queue=executor.queue(), dispatcher_lock=self._dispatcher_lock)
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
                # Note: *executor* (sms.execution.RuntimeManager) returns itself when
                # "entered", then we yield it below. Now that RuntimeManager is
                # fairly normalized, we could pass the dispatcher to a (new)
                # context manager member function
                # and let the RuntimeManager handle all of this *dispatcher* logic.
                # The WorkflowManager could pass itself as a simpler interface
                # * to the Queuer for the `subscribe` add_item hook and
                # * to the RuntimeManager to provide a WorkflowEditor.edit_item.
                # E.g.
                #     @asynccontextmanager
                #     async def RuntimeManager.manage(
                #       dispatcher: Queuer,
                #       subscriber: WorkflowEditor))
                # Consider also the similarity of RuntimeManager-WorkflowManager-Queuer
                # to a Model-View-Controller.
                async with dispatcher:
                    # We can surrender control here and leave the executor and
                    # dispatcher tasks active while evaluating a `with` block suite
                    # for the `dispatch` context manager.
                    yield dispatching_session
                    # When leaving the `with` suite, Queuer.__aexit__ sends a *stop*
                    # command to the queue.
                # The *stop* command will be picked up by sms.execution.manage_execution()
                # (as the RuntimeManager's *runner_task*), which will be awaited in
                # RuntimeManager.__exit__().

        except Exception as e:
            logger.exception("Unhandled exception while in dispatching context.")
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
                    logger.info("Dispatching queue processor cancelled.")
                else:
                    assert not isinstance(dispatcher_exception, asyncio.CancelledError)
                    logger.exception("Queuer encountered exception.", exc_info=dispatcher_exception)
            else:
                if not dispatcher.queue().empty():
                    logger.error(
                        "Queuer finished while items remain in dispatcher queue. "
                        "Approximate size: {}".format(dispatcher.queue().qsize())
                    )

            executor_exception = executor.exception()
            if executor_exception:
                if isinstance(executor_exception, asyncio.CancelledError):
                    logger.info("Executor cancelled.")
                else:
                    assert not isinstance(executor_exception, asyncio.CancelledError)
                    logger.exception("Executor task finished with exception", exc_info=executor_exception)
            else:
                if not executor.queue().empty():
                    # TODO: Handle non-empty queue.
                    # There are various reasons that the queue might not be empty and
                    # we should clean up properly instead of bailing out or compounding
                    # exceptions.
                    # TODO: Check for extraneous extra *stop* commands.
                    logger.error("Bug: Executor left tasks in the queue without raising an exception.")

            logger.debug("Exiting {} dispatch context.".format(type(self).__name__))

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

    def add_item(self, item) -> ItemView:
        """
        Add an item to the managed workflow.

        Args:
            item: Describe the item to be managed. (Usually a task to be submitted.)

        Returns:
            View of the managed item.

        """
        if self.closed:
            raise ScopeError("WorkflowManager is closed.")

        # # TODO: Resolve implementation details for *operation*.
        # if operation != 'scalems.executable':
        #     raise MissingImplementationError('No implementation for {} in {}'.format(
        #     operation, repr(self)))
        # # Copy a static copy of the input.
        # # TODO: Dispatch tasks addition, allowing negotiation of Context capabilities
        #  and subscription
        # #  to resources owned by other Contexts.
        # if not isinstance(bound_input, scalems.subprocess.SubprocessInput):
        #     raise ValueError('Only scalems.subprocess.SubprocessInput objects
        #     supported as input.')

        # TODO: Replace with type-based dispatching or some normative interface test.
        from scalems.subprocess import Subprocess

        if not isinstance(item, (Subprocess, dict)):
            raise MissingImplementationError("Operation not supported.")

        # TODO: Generalize interface check.
        if isinstance(item, Subprocess):
            uid: bytes = item.uid()
            if uid in self.tasks:
                # TODO: Consider decreasing error level to `warning`.
                raise DuplicateKeyError("Task already present in workflow.")
            logger.debug("Adding {} to {}".format(str(item), str(self)))
            record = {"uid": uid.hex(), "type": item.resource_type().scoped_identifier(), "input": {}}
            task_input = item.input_collection()
            for field in dataclasses.fields(task_input):
                name = field.name
                try:
                    # TODO: Need serialization typing.
                    record["input"][name] = getattr(task_input, name)
                except AttributeError as e:
                    raise InternalError("Unexpected missing field.") from e
        else:
            # WARNING: no coverage
            assert isinstance(item, dict)
            assert "uid" in item
            uid = item["uid"]
            implementation_identifier = item.get("implementation", None)
            if not isinstance(implementation_identifier, list):
                raise DispatchError("Bug: bad schema checking?")

            if uid in self.tasks:
                # TODO: Consider decreasing error level to `warning`.
                raise DuplicateKeyError("Task already present in workflow.")
            logger.debug("Adding {} to {}".format(str(item), str(self)))
            record = {"uid": uid.hex(), "type": tuple(implementation_identifier), "input": item}
        serialized_record = json.dumps(record, default=encode)

        # TODO: Make sure there are no artifacts of shallow copies that may result in
        #       a user modifying nested objects unexpectedly.
        item = Task(self, serialized_record)
        # TODO: Check for ability to dispatch.
        #  Note module dependencies, etc. and check in target execution environment
        #  (e.g. https://docs.python.org/3/library/importlib.html#checking-if-a-module
        #  -can-be-imported)

        # TODO: Provide a data descriptor and possibly a more formal Workflow class.
        # We do not yet check that the derived classes actually initialize self.tasks.
        self.tasks[uid] = item

        task_view = ItemView(manager=self, uid=uid)

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
        for callback in self._event_hooks["add_item"]:
            # TODO: Do we need to provide a contextvars.Context object to the callback?
            logger.debug(f"Running dispatching hook for add_item subscriber {repr(callback)}.")
            callback(CommandQueueAddItem({"add_item": uid}))

        return task_view

    def datastore(self):
        """Get a handle to the non-volatile backing data store."""
        if self.closed:
            raise ScopeError("WorkflowManager is closed.")
        return self._filestoremanager.filestore()


@functools.singledispatch
def workflow_item_director_factory(
    item, *, manager: WorkflowManager, label: str = None
) -> typing.Callable[..., ItemView]:
    """Get a workflow item director for a workflow manager and input type.

    When called, the director finalizes the new item and returns a view.
    """
    # TODO: Incorporate into WorkflowManager interface as a singledispatchmethod.
    # Design note: WorkflowManager classes could cache implementation details for item
    # types,
    # but (since we acknowledge that work may be defined before instantiating the
    # context to
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
    raise MissingImplementationError("No registered implementation for {} in {}".format(repr(item), repr(manager)))


@workflow_item_director_factory.register
def _(item_type: type, *, manager: WorkflowManager, label: str = None) -> typing.Callable[..., ItemView]:
    # TODO: Do we really want to handle dispatching on type _or_ instance args?
    # When dispatching on a class instead of an instance, just construct an
    # object of the indicated type and re-dispatch. Note that implementers of
    # item types must register an overload with this singledispatch function.
    # TODO: Consider how best to register operations and manage their state.
    def constructor_proxy_director(*args, **kwargs) -> ItemView:
        if not isinstance(item_type, type):
            raise ProtocolError(
                "This function is intended for a dispatching path on which *item_type* is a `type` object."
            )
        item = item_type(*args, **kwargs)
        director = workflow_item_director_factory(item, manager=manager, label=label)
        return director()

    return constructor_proxy_director


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

    source: WorkflowManager
    """Owner of the workflow items being queued."""

    _dispatcher_lock: asyncio.Lock
    """Provided by caller to allow safe transitions of dispatching state."""

    _dispatcher_queue: _queue.SimpleQueue

    _queue_runner_task: asyncio.Task
    """Queue, owned by this object, of Workflow Items being processed for dispatch."""

    def __init__(self, source: WorkflowManager, command_queue: asyncio.Queue, dispatcher_lock=None):
        """Create a queue-based workflow dispatcher.

        Initialization and deinitialization occurs through
        the Python (async) context manager protocol.
        """

        self.source = source
        self._dispatcher_queue = _queue.SimpleQueue()
        self.command_queue = command_queue

        if not isinstance(dispatcher_lock, asyncio.Lock):
            raise TypeError("An asyncio.Lock is required to control dispatcher state.")
        self._dispatcher_lock = dispatcher_lock

        self._exception = None

    def queue(self):
        return self._dispatcher_queue

    def put(self, item: typing.Union[CommandQueueAddItem, CommandQueueControlItem]):
        assert len(item) == 1
        key = list(item.keys())[0]
        if key not in {"command", "add_item"}:
            raise APIError("Unrecognized queue item representation.")
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
                    raise APIError("There is already an active dispatcher in this Context.")
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
                self.source.subscribe("add_item", self.put)
                # TODO: Topologically sort DAG!
                initial_task_list = list(self.source.tasks.keys())
                try:
                    for _task_id in initial_task_list:
                        self.command_queue.put_nowait(QueueItem({"add_item": _task_id}))
                except asyncio.QueueFull as e:
                    raise DispatchError("Executor was unable to receive initial commands.") from e
                # It is now safe to yield.

                # TODO: Add lock context for WorkflowManager event hooks
                #  rather than assume the UI and event loop are always in the same thread.

            return self
        except Exception as e:
            self._exception = e
            raise e

    async def _single_iteration_queue(self, source: _queue.SimpleQueue, target: asyncio.Queue):
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
        logger.debug(f"Processing command {repr(command)}")

        await target.put(command)

        # TODO: Use formal RPC protocol.
        if "control" in command:
            # Note that we don't necessarily need to stop managing the dispatcher queue
            # at this point, but the Executor will be directed to shut down,
            # so we must not put anything else onto the command queue until we have a
            # new command queue or a new executor.
            if command["control"] == "stop":
                raise asyncio.CancelledError()
            else:
                raise ProtocolError("Unknown command: {}".format(command["control"]))
        else:
            if "add_item" not in command:
                # TODO: We might want a call-back or Event to force errors before the
                #  queue-runner task is awaited.
                raise MissingImplementationError(f"Executor has no implementation for {str(command)}")
        return command

    async def _queue_runner(self, processing_state: asyncio.Event):
        processing_state.set()
        while True:
            try:
                await asyncio.shield(
                    self._single_iteration_queue(source=self._dispatcher_queue, target=self.command_queue)
                )
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
        # The dispatching protocol is immature. Initially, we don't expect contention
        # for the lock, and if there is contention, it probably represents an
        # unintended race condition or systematic dead-lock.
        # TODO: Clarify dispatcher state machine and remove/replace assertions.
        assert not self._dispatcher_lock.locked()
        async with self._dispatcher_lock:
            try:
                self.source.unsubscribe("add_item", self.put)
                _dispatcher.set(None)

                # Stop the dispatcher.
                logger.debug("Stopping the SCALEMS RP dispatching queue runner.")

                # Wait for the queue to drain or the queue runner to exit or fail.
                drain = asyncio.create_task(self._drain_queue())
                done, pending = await asyncio.wait(
                    {drain, self._queue_runner_task}, return_when=asyncio.FIRST_COMPLETED
                )
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
                    logger.exception("Queuer queue processing encountered exception", exc_info=exception)
                    if self._exception:
                        logger.error("Queuer is already holding an exception.")
                    else:
                        self._exception = exception

            except asyncio.CancelledError as e:
                logger.debug("Queuer context manager received cancellation while exiting.")
                cancelled_error = e
            except Exception as e:
                logger.exception("Exception while stopping dispatcher.", exc_info=e)
                if self._exception:
                    logger.error("Queuer is already holding an exception.")
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
    #     #     warnings.warn('Unsuccessful tasks: ' + ', '.join([repr(t) for t in
    #     failures]))
    #
    #     yield
    #     if self._exception:
    #         raise self._exception
    #
    #     # # If we want to provide a "Future-like" interface, we should support the
    #     callback
    #     # # protocols and implement the following generator function.
    #     # if not self.done():
    #     #     self._asyncio_future_blocking = True
    #     #     # ref https://docs.python.org/3/library/asyncio-future.html#asyncio
    #     .isfuture
    #     #
    #     #     yield self  # This tells Task to wait for completion.
    #     # if not self.done():
    #     #     raise RuntimeError("The dispatcher task was not 'await'ed.")
    #     # Ref PEP-0380: "return expr in a generator causes StopIteration(expr)
    #     # to be raised upon exit from the generator."
    #     # The Task works like a `result = yield from awaitable` expression.
    #     # The iterator (generator) yields until exhausted,
    #     # then raises StopIteration with the value returned in by the generator
    #     function.
    #     # return self.result()  # May raise too.
    #     # # Otherwise, the only allowed value from the iterator is None.


_shared_scope_lock = threading.RLock()

_shared_scope_count = contextvars.ContextVar("_shared_scope_count", default=0)

_dispatcher: contextvars.ContextVar = contextvars.ContextVar("_dispatcher")
"""Identify an asynchronous Context.

Non-asyncio-aware functions may need to behave
differently when we know that asynchronous context switching could happen.
We allow multiple dispatchers to be active, but each dispatcher must
1. contextvars.copy_context()
2. set itself as the dispatcher in the new Context.
3. run within the new Context.
4. ensure the Context is destroyed (remove circular references)
"""

current_scope: contextvars.ContextVar = contextvars.ContextVar("current_scope")
"""The active workflow manager, if any.

This property is global within the interpreter or a `contextvars.Context`.

Note that it makes no sense to start a dispatching session without concurrency,
so we can think in terms of a parent context doing contextvars.copy_context().run(...),
but the parent must set the current_scope correctly in the new Context.
"""


def get_scope():
    """Get a reference to the manager of the current workflow scope."""
    # get_scope() is used to determine the default workflow manager when *context*
    # is not provided to scalems object factories, scalems.run(), scalems.wait() and
    # (non-async) `result()` methods.
    # Async coroutines can safely use get_scope(), but should not use the
    # non-async scope() context manager for nested scopes without wrapping
    # in a contextvars.run() that locally resets the Context current_scope.
    try:
        _scope: typing.Union[WorkflowManager, weakref.ReferenceType] = current_scope.get()
        if isinstance(_scope, weakref.ReferenceType):
            _scope = _scope()
        manager: WorkflowManager = _scope
        # This check is in case we use weakref.ref:
        if manager is None:
            raise ProtocolError("Context for current scope seems to have disappeared.")
        logger.debug(f"Scope queried with get_scope() {repr(manager)}")
    except LookupError:
        logger.debug("Scope was queried, but has not yet been set.")
        return None
    return manager


@contextlib.contextmanager
def scope(manager, close_on_exit=False):
    """Set the current workflow management within a clear scope.

    Restores the previous workflow management scope on exiting the context manager.

    To allow better tracking of dispatching chains, this context manager does not allow
    the global workflow management scope to be "stolen". If *manager* is already the
    current scope, a recursion depth is tracked, and the previous scope is restored
    only when the last "scope" context manager for *manager* exits. Multiple "scope"
    context managers are not allowed for different *manager* instances in the same
    :py:class:`context <contextvars.Context>`.

    If *close_on_exit=True*, calls ``manager.close()`` when leaving *manager's*
    scope for the last time, if possible.

    Note:
        Scope indicates the "active" WorkflowManager instance.
        This is separate from WorkflowManager lifetime and ownership.
        WorkflowManagers should track their own activation status and provide logic for
        whether to allow reentrant dispatching.

    Within the context managed by *scope*, get_scope() will return *context*.

    While this context manager should be thread-safe, in general, this context manager
    should only be used in the root thread where the UI and event loop are running to
    ensure we can clean up properly.
    Dispatchers may provide environments in which this context manager can be used in
    non-root threads, but the Dispatcher needs to curate the contextvars.ContextVars
    and ensure that the Context is properly cleaned up.
    """
    _close = None
    if close_on_exit:
        _close = getattr(manager, "close", None)
        if not callable(_close):
            raise ValueError("close_on_exit is True, but manager has no close() method.")

    logger.debug(f"Request to enter the scope of {manager}.")
    with _shared_scope_lock:
        parent = get_scope()
        dispatcher = _dispatcher.get(None)
        # A dispatcher can explicitly allow or disallow nested scopes by setting
        # current_scope to itself or something else.
        if parent is None or parent is dispatcher or parent is manager:
            if parent is not manager:
                logger.debug("Entering scope of {}".format(str(manager)))
            token = current_scope.set(weakref.ref(manager))
            _shared_scope_count.set(_shared_scope_count.get() + 1)
        else:
            assert dispatcher is not None
            raise ProtocolError(f"Cannot nest {manager} scope in {parent} scope while dispatching under {dispatcher}.")
        try:
            yield manager
        finally:
            _shared_scope_count.set(_shared_scope_count.get() - 1)
            if _shared_scope_count.get() == 0:
                logger.debug("Leaving scope of {}".format(str(manager)))
                token.var.reset(token)
                if _close:
                    _close()
    # If we need to have multiple nested scopes across different threads, we can
    # hold the lock only for __enter__ and __exit__, but we will need to get the
    # lock object from a ContextVar or from the dispatcher. Then the above `try`
    # block would be moved up a level and would look something like the following.
    # try:
    #     yield manager
    # finally:
    #     with _shared_scope_lock:
    #         _shared_scope_count.set(_shared_scope_count.get() - 1)
    #         if _shared_scope_count.get() == 0:
    #             logger.debug('Leaving scope of {}'.format(str(manager)))
    #             token.var.reset(token)


def _unpack_work(ref: dict):
    """Temporary handler for ad hoc dict-based input.

    Unpack and serialize the nested task descriptions.

    Note: this assumes work is nested, with only one item per "layer".
    """
    assert isinstance(ref, dict)
    implementation_identifier = ref.get("implementation", None)
    message: dict = ref.get("message", None)
    if not isinstance(implementation_identifier, list) or not isinstance(message, dict):
        raise _exceptions.DispatchError("Bug: bad schema checking?")

    command = implementation_identifier[-1]
    logger.debug(f"Unpacking a {command}")
    # Temporary hack for ad hoc schema.
    if command == "Executable":
        # generate Subprocess
        from scalems.subprocess import SubprocessInput, Subprocess

        input_node, task_node, output_node = message["Executable"]
        kwargs = {
            "argv": input_node["data"]["argv"],
            "stdin": input_node["data"]["stdin"],
            "stdout": task_node["data"]["stdout"],
            "stderr": task_node["data"]["stderr"],
            "environment": input_node["data"]["environment"],
            "resources": task_node["input"]["resources"],
        }
        bound_input = SubprocessInput(**kwargs)
        item = Subprocess(input=bound_input)
        yield item
        return item.uid()
    else:
        # If record bundles dependencies, identify them and yield them first.
        try:
            depends = ref["message"][command]["input"]
        except AttributeError:
            depends = None
        if depends is not None:
            logger.debug(f"Recursively unpacking {depends}")
            dependency: typing.Optional[bytes] = yield from _unpack_work(depends)
        else:
            dependency = None
        if "uid" not in ref:
            ref["uid"] = next_monotonic_integer().to_bytes(32, "big")
        uid: bytes = ref["uid"]
        if dependency is not None:
            logger.debug("Replacing explicit input in {} with reference: {}".format(uid.hex(), dependency.hex()))
            ref["message"][command]["input"] = dependency
        # Then yield the dependent item.
        yield ref
        return uid


@functools.singledispatch
def _wait(ref, *, manager):
    """Use the indicated workflow manager to resolve a reference to a workflow item."""
    raise _exceptions.DispatchError("No dispatcher for this type of reference.")
    # TODO: Return an object supporting the result type interface.


@_wait.register
def _(ref: dict, *, manager):
    # First draft: monolithic implementation directs the workflow manager to add tasks and execute them.
    # TODO: Use a WorkflowManager interface from the core data model.
    if not isinstance(manager, WorkflowManager):
        raise _exceptions.ProtocolError("Provided manager does not implement the required interface.")
    for item in _unpack_work(ref):
        view = manager.add_item(item)
        logger.debug("Added {}: {}".format(view.uid().hex(), str(item)))
    # TODO: If dispatcher is running, wait for the results.
    # TODO: If dispatcher is not running, can we trigger it?


def wait(ref):
    """Resolve a workflow reference to a local object.

    *wait* signals to the SCALE-MS framework that it is time to intervene and
    do some workflow execution management.

    ScaleMS commands return abstract references to work without waiting for the
    work to execute. Other ScaleMS commands can operate on these references,
    relying on the framework to manage data flow.

    If you need to extract a concrete result, or otherwise force data flow resolution
    (blocking the current code until execution and data transfer are complete),
    you may use scalems.wait(ref) to convert a workflow reference to a concrete
    local result.

    Note that scalems.wait() can allow the current scope to yield to other tasks.
    Developers should use scalems.wait() instead of native concurrency primitives
    when coding for dynamic data flow.
    However, the initial implementation does not inspect the context to allow
    such context-sensitive behavior.

    .. todo:: Establish stable API/CPI for tasks that create other tasks or modify the data flow graph
        during execution.

    scalems.wait() will produce an error if you have not configured and launched
    an execution manager in the current scope.

    .. todo:: Acquire asyncio event loop from WorkflowManager.
        scalems.wait is primarily intended as an abstraction from
        https://docs.python.org/3.8/library/asyncio-eventloop.html#asyncio.loop.run_until_complete
        and an alternative to `await`.
    """
    context = get_scope()
    if context is None:
        # Bail out.
        raise _exceptions.DispatchError(str(ref))
    if not isinstance(context, WorkflowManager):
        raise _exceptions.ProtocolError("Expected WorkflowManager. Got {}".format(repr(context)))

    # Dispatch on reference type.
    return _wait(ref, manager=context)
    # TODO: Normalize a Future behavior. Require argument types that guarantee
    #   that the work will eventually be handled, and defer to asyncio.wait, or
    #   reliably produce an error if this is not possible.
