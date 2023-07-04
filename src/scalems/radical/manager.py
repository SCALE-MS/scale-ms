"""RADICAL Pilot runtime management for the scalems client."""

from __future__ import annotations

__all__ = ("launch", "RuntimeManager")

import asyncio
import atexit
import contextlib
import logging
import os
import threading
import typing
import weakref

import scalems.exceptions
import scalems.execution
import scalems.messages
import scalems.workflow
import scalems.radical.raptor
from scalems.exceptions import DispatchError
from scalems.identifiers import EphemeralIdentifier
from scalems.radical.runtime_configuration import RuntimeConfiguration
from scalems.radical.session import runtime_session
from scalems.radical.session import RuntimeSession
from scalems.store import FileStore

if typing.TYPE_CHECKING:
    from radical import pilot as rp

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


# The following infrastructure logic is borrowed from concurrent.futures.thread
# What we really want, though, is to make sure that these hooks are handled before
#  the event loop is closed.
# TODO: Make sure that event loop is still active, and perform our shutdown hooks
#  before the event loop shuts down.
_runtime_queues = weakref.WeakKeyDictionary()
"""Keep track of the active executor queues in the current runtime.

Analogous to the way concurrent.futures.thread tracks the ThreadPoolExecutor
threads and work queues.
"""

_shutdown = False
# Lock that ensures that new executors are not created while the interpreter is
# shutting down. Must be held while mutating _runtime_queues and _shutdown.
# Warning: don't copy this while locked, such as by forking.
_global_shutdown_lock = threading.Lock()


def _python_exit():
    global _shutdown
    with _global_shutdown_lock:
        _shutdown = True
    items = list(_runtime_queues.items())
    # TODO: Update to something that will allow completion of anything that was
    #  trying to get the _global_shutdown_lock or that is affected by _shutdown=True
    for t, q in items:
        q.put(None)
    for t, q in items:
        t.join()


atexit.register(_python_exit)


class RuntimeManager:
    """Runtime resources for SCALE-MS workflow execution, backed by RADICAL Pilot.

    Initialize with a Session and pre-configured Pilot job.

    Executors are provisioned in the allocated resources and user configuration
    of a RuntimeManager.

    The RuntimeManager mediates interaction between Executors and WorkflowManager
    to fulfil Futures and update workflow state.

    We expect to have Executor state somewhat disconnected from the calling program
    scope, so the RuntimeManager context is an opportunity to collect Tasks bound
    to different executors, to cleanly shut down executors and release resources.

    The RuntimeManager implements the Computing Provider Interface (CPI) for SCALE-MS
    calls to the runtime provider.`
    """

    runtime_configuration: RuntimeConfiguration
    runtime_session: RuntimeSession = scalems.execution.RuntimeDescriptor()

    def __init__(
        self,
        *,
        datastore: FileStore = None,
        loop: asyncio.AbstractEventLoop,
        configuration: RuntimeConfiguration,
        runtime: scalems.radical.session.RuntimeSession,
    ):
        if not isinstance(configuration.target_venv, str) or len(configuration.target_venv) == 0:
            raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

        self._exception = None
        self._loop: asyncio.AbstractEventLoop = loop
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

        # TODO: Connect with WorkflowManager to query workflow state and to provide Task updates.
        # if editor_factory is None or not callable(editor_factory):
        #     raise TypeError("Provide a callable that produces an edit_item interface.")
        # self.get_edit_item = editor_factory

        if datastore is None:
            raise TypeError("Provide a datastore.")
        self.datastore = datastore

        self.runtime_configuration = configuration

        self.runtime_session = runtime

    @staticmethod
    async def cpi(command: str, runtime: RuntimeSession):
        """Send a control command to the raptor scheduler.

        Implements :py:func:`scalems.execution.RuntimeManager.cpi()`

        TODO: Unify with new raptor cpi feature.
        """
        timeout = 180

        logger.debug(f'Received command "{command}" for runtime {runtime}.')
        if runtime is None:
            raise scalems.exceptions.ScopeError("Cannot issue control commands without an active RuntimeManager.")
        raptor: rp.Task = runtime.raptor
        logger.debug(f"Preparing command for {repr(raptor)}.")
        if raptor is None:
            raise scalems.exceptions.ScopeError(
                f"Cannot issue control commands without an active RuntimeManager. {runtime}"
            )
        if raptor.state in rp.FINAL:
            raise scalems.exceptions.ScopeError(f"Raptor scheduler is not available. {repr(raptor)}")
        assert raptor.uid
        message = scalems.messages.Control.create(command)
        td = rp.TaskDescription(
            from_dict={
                "raptor_id": raptor.uid,
                "mode": scalems.radical.raptor.CPI_MESSAGE,
                "metadata": message.encode(),
                "uid": EphemeralIdentifier(),
            }
        )
        logger.debug(f"Submitting {str(td.as_dict())}")
        (task,) = await asyncio.to_thread(runtime.task_manager().submit_tasks, [td])
        logger.debug(f"Submitted {str(task.as_dict())}. Waiting...")
        # Warning: we can't wait on the final state of such an rp.Task unless we
        # _also_ watch the scheduler itself, because there are various circumstances
        # in which the Task may never reach a rp.FINAL state.

        command_watcher = asyncio.create_task(
            asyncio.to_thread(task.wait, state=rp.FINAL, timeout=timeout), name="cpi-watcher"
        )

        raptor_watcher = asyncio.create_task(
            asyncio.to_thread(raptor.wait, state=rp.FINAL, timeout=timeout), name="raptor-watcher"
        )
        # If raptor task fails, command-watcher will never complete.
        done, pending = await asyncio.wait(
            (command_watcher, raptor_watcher), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if raptor_watcher in done and command_watcher in pending:
            command_watcher.cancel()
        logger.debug(str(task.as_dict()))
        # WARNING: Dropping the Task reference will cause its deletion.
        return command_watcher

    async def wait_closed(self):
        return await self.runtime_session.wait_closed()

    def close(self):
        """Manage tear down of the RADICAL Pilot Session and resources.

        Several aspects of the RP runtime interface use blocking calls.
        This method should be run in a non-root thread (concurrent.futures.Future)
        that the event loop can manage as an asyncio-wrapped task.

        The method should be used along with the wait_closed() method to allow
        the asyncio event loop to clean up.

        Overrides :py:class:`scalems.execution.RuntimeManager`

        Design note:
            Note that shutdown could take a while. To avoid blocking the event loop,
            we need a consistent strategy for dispatching to threads. We need to
            make sure that we can shut down as cleanly as possible even if the
            asyncio default ThreadPoolExecutor cannot provide an available thread.
            will probably dispatch it to a thread.

            For now, we will try to keep a strategy of performing high level lifetime
            management calls (like `close()`) exclusively from the event loop thread,
            and moving blocking calls to a thread pool at the implementation level.
            If this strategy becomes impractical, we will have to take care when
            mixing `concurrent.futures` logic and `asyncio` logic.

        """
        # WARNING: If this call needs to manipulate ContexVars, we need to be
        #   careful about how it is dispatched by the caller.

        with self.shutdown_lock():
            if self._shutdown:
                return
            self._shutdown = True
        runtime: RuntimeSession = self.runtime_session

        # TODO: Collect or cancel outstanding tasks, shut down executors, and
        #  synchronize the workflow state.

        # Resolve any Tasks in this runtime scope and make sure their executor
        # resources are freed.
        ...

        # Note that this coroutine could take a long time and could be
        # cancelled at several points.

        try:
            # TODO: Stop the executors.
            # logger.debug("Stopping workflow execution.")
            ...
        # except asyncio.CancelledError as e:
        #     logger.debug(f"{self.__class__.__qualname__} context manager received cancellation while exiting.")
        #     cancelled_error = e
        # except Exception as e:
        #     logger.exception("Exception while stopping dispatcher.", exc_info=True)
        #     if self._exception:
        #         logger.error("Queuer is already holding an exception.")
        #     else:
        #         self._exception = e
        finally:
            # Catch and report errors from executors, and make any necessary updates to workflow state.
            ...

        session: rp.Session = getattr(runtime, "session", None)
        if session is None:
            raise scalems.exceptions.APIError(f"No Session in {runtime}.")
        if session.closed:
            logger.error("RuntimeSession is already closed?!")
        else:
            runtime.close()

            if session.closed:
                logger.debug(f"Session {session.uid} closed.")
            else:
                logger.error(f"Session {session.uid} not closed!")
        logger.debug("RuntimeSession shut down.")
        # TODO: Disconnect from WorkflowManager.

    @contextlib.contextmanager
    def shutdown_lock(self):
        with _global_shutdown_lock, self._shutdown_lock:
            if _shutdown:
                raise RuntimeError("Shutdown is already in progress.")
            yield self._shutdown_lock

    async def __aenter__(self):
        if not _shutdown:
            return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: C901
        """Clean up at context exit.

        In addition to handling exceptions, clean up any Session resource.

        We also need to make sure that we properly disengage from any queues
        or generators.

        We can also leave flags for ourself to be checked at __await__,
        if there is a Task associated with the Executor.
        """
        # First, do something with exceptions that occurred in the `with` block if we're going to.
        ...
        # Finally, shut down the runtime.
        try:
            self.close()
            await self.wait_closed()
        except Exception as e:
            logger.exception("Exception during RuntimeManager.close().")
            raise e

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False


@contextlib.asynccontextmanager
async def launch(
    workflow_manager: scalems.workflow.WorkflowManager, runtime_configuration: RuntimeConfiguration
) -> RuntimeManager:
    """Get a runtime execution context for the managed workflow.

    Configure, launch, and manage the runtime facilities.
    The resulting RuntimeManager context can be used to acquire an
    Executor for handling task submissions.

    Example::

        with scalems.workflow.scope(workflow, close_on_exit=True):
            async with scalems.radical.runtime.launch(workflow, runtime_config) as runtime_context:
                async with scalems.execution.executor(runtime_context, requirements={}) as executor:
                    task: concurrent.futures.Future = executor.submit(fn, *args, **kwargs)
                    task: asyncio.Task = asyncio.create_task(loop.run_in_executor(executor, fn, *args))

    """
    if workflow_manager.closed:
        raise scalems.exceptions.ScopeError("WorkflowManager is closed.")

    # Get Workflow editor factory or connect to WorkflowManager to provide Task updates.
    # TODO: Integrate with WorkflowManager.

    # RuntimeManager.__init__
    if "RADICAL_PILOT_DBURL" not in os.environ:
        raise DispatchError("RADICAL Pilot environment is not available.")

    if not isinstance(runtime_configuration.target_venv, str) or len(runtime_configuration.target_venv) == 0:
        raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

    # self.submitted_tasks = set()
    #
    # # TODO: Only hold a queue in an active context manager.
    # self._command_queue = asyncio.Queue()
    # self._exception = None
    # self._loop: asyncio.AbstractEventLoop = loop
    #
    # if editor_factory is None or not callable(editor_factory):
    #     raise TypeError("Provide a callable that produces an edit_item interface.")
    # self.get_edit_item = editor_factory
    #
    # if datastore is None:
    #     raise TypeError("Provide a datastore.")
    # self.datastore = datastore
    #
    # # TODO: Consider relying on module ContextVars and contextvars.Context scope.
    # self._runtime_configuration = configuration
    #
    # if not isinstance(dispatcher_lock, asyncio.Lock):
    #     raise TypeError("An asyncio.Lock is required to control dispatcher state.")
    # self._dispatcher_lock = dispatcher_lock

    # RuntimeManager.__aenter__
    # Get a lock while the state is changing.
    # Warning: The dispatching protocol is immature.
    # Initially, we don't expect contention for the lock, and if there is
    # contention, it probably represents an unintended race condition
    # or systematic dead-lock.
    # TODO: Clarify dispatcher state machine and remove/replace assertions.
    with _global_shutdown_lock:
        if _shutdown:
            return
        # TODO: Check that we have a FileStore.

        # Note: the current implementation implies that only one Task for the dispatcher
        # will exist at a time. We are further assuming that there will probably only
        # be one Task per the lifetime of the dispatcher object.
        # We could choose another approach and change our assumptions, if appropriate.
        logger.debug("Entering RP dispatching context. Waiting for rp.Session.")

        # Note that any thread-dispatched creation functions or other subtasks
        # in `runtime_session` will see the contextvars.Context state from _before_
        # RuntimeManager takes control of its scope. Be careful to keep in mind
        # appropriate coupling and collective state transitions of RuntimeSession
        # and RuntimeManager.
        _runtime: RuntimeSession = await runtime_session(
            configuration=runtime_configuration, loop=workflow_manager.loop()
        )

        # Raptor session will be initialized and attached in the executor context, if needed.
        assert _runtime.raptor is None

        runtime_manager = RuntimeManager(
            datastore=workflow_manager.datastore(),
            loop=workflow_manager.loop(),
            configuration=runtime_configuration,
            runtime=_runtime,
        )
        # TODO: Don't release the _global_shutdown_lock until we have set any
        #  necessary variables for program state, but don't yield
        #  with the lock held.

    # We wrap the RuntimeManager context manager to make it easier to dispatch
    # `close()` to a separate thread and to handle errors, while keeping
    # `RuntimeManager.close()` as simple as possible.
    async with runtime_manager as runtime_context:
        if runtime_context is not None:
            try:
                yield runtime_context

            except Exception as e:
                logger.exception("Unhandled exception while in runtime manager context.")
                raise e

            finally:
                close_task = asyncio.create_task(asyncio.to_thread(runtime_context.close), name="runtime_manager.close")
                # __aexit__
                # runtime_shutdown
                await close_task
                # runtime_exception = runtime_context.exception()
                # if runtime_exception:
                #     if isinstance(runtime_exception, asyncio.CancelledError):
                #         logger.info("Executor cancelled.")
                #     else:
                #         assert not isinstance(runtime_exception, asyncio.CancelledError)
                #         logger.exception("Executor task finished with exception", exc_info=runtime_exception)
                # else:
                #     if not runtime_context.queue().empty():
                #         # TODO: Handle non-empty queue.
                #         # There are various reasons that the queue might not be empty and
                #         # we should clean up properly instead of bailing out or compounding
                #         # exceptions.
                #         # TODO: Check for extraneous extra *stop* commands.
                #         logger.error("Bug: Executor left tasks in the queue without raising an exception.")

                logger.debug(f"Exited {runtime_manager} context.")
