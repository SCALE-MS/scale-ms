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

__all__ = ("WorkflowManager", "get_scope", "scope", "workflow_item_director_factory")

import asyncio
import contextlib
import contextvars
import dataclasses
import functools
import json
import logging
import os
import threading
import typing
import weakref

from scalems.store import FileStoreManager
from scalems.messages import CommandQueueAddItem
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
        # TODO: Establish and check the serialization scheme / JSON schema.
        # I.e. the result type of Task.serialize()
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

    The WorkflowManager maintains the local metadata and provides the representation
    of workflow state. The workflow can be edited and state updated through
    several collaborations, but the WorkflowManager is not directly responsible
    for the dispatching of tasks for execution.

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

    # TODO: Consider a threading.Lock for editing permissions.
    # TODO: Consider asyncio.Lock instances for non-thread-safe state updates during
    #  execution and dispatching.

    def __init__(self, *, loop: asyncio.AbstractEventLoop, directory: typing.Union[str, os.PathLike] = None):
        """
        The event loop for the program should be launched in the root thread,
        preferably early in the application launch.
        Whether the WorkflowManager uses it directly,
        it is useful to require the client to provide the event loop,
        if for no other reason than to ensure that one exists.

        Args:
            loop: event loop, such as from asyncio.new_event_loop()
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
        # TODO: Check for subscribers.
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


_shared_scope_lock = threading.RLock()

_shared_scope_count = contextvars.ContextVar("_shared_scope_count", default=0)

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
    # TODO: Only one active WorkflowManager, though possibly multiple executors.
    with _shared_scope_lock:
        parent = get_scope()
        if parent is None or parent is manager:
            if parent is not manager:
                logger.debug("Entering scope of {}".format(str(manager)))
            token = current_scope.set(weakref.ref(manager))
            _shared_scope_count.set(_shared_scope_count.get() + 1)
        else:
            raise ProtocolError(f"Cannot nest {manager} scope in {parent} scope.")
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
