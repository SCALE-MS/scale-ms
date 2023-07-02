from __future__ import annotations

__all__ = ("launch", "RuntimeManager")

import asyncio
import atexit
import contextlib
import logging
import os
import threading
import weakref

import scalems.exceptions
import scalems.execution
import scalems.messages
import scalems.workflow
import scalems.radical.raptor
from scalems.exceptions import DispatchError
from scalems.radical.runtime_configuration import RuntimeConfiguration
from scalems.radical.session import runtime_session
from scalems.radical.session import RuntimeSession
from scalems.store import FileStore

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
    # TODO: Update
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

        if datastore is None:
            raise TypeError("Provide a datastore.")
        self.datastore = datastore

        self.runtime_configuration = configuration

        self.runtime_session = runtime

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

        runtime: RuntimeSession = self.runtime_session

        # TODO: Collect or cancel outstanding tasks, shut down executors, and
        #  synchronize the workflow state.

        runtime.close()
        logger.debug("RuntimeSession shut down.")

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

    if "RADICAL_PILOT_DBURL" not in os.environ:
        raise DispatchError("RADICAL Pilot environment is not available.")

    if not isinstance(runtime_configuration.target_venv, str) or len(runtime_configuration.target_venv) == 0:
        raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

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

        # TODO: Asynchronous data staging optimization.
        #  Allow input data staging to begin before scheduler is in state EXECUTING and
        #  before Pilot is in state PMGR_ACTIVE.

        # Raptor session will be initialized and attached in the executor context, if needed.
        assert _runtime.raptor is None

        runtime_context = RuntimeManager(
            datastore=workflow_manager.datastore(),
            loop=workflow_manager.loop(),
            configuration=runtime_configuration,
            runtime=_runtime,
        )

    try:
        # Manage scope of executor operation with a context manager.
        # RP does not yet use an event loop, but we can use async context manager
        # for future compatibility with asyncio management of network connections,
        # etc.
        #
        # Note: the executor owns a rp.Session during operation.
        yield runtime_context

    except Exception as e:
        logger.exception("Unhandled exception while in dispatching context.")
        raise e

    finally:
        close_task = asyncio.create_task(asyncio.to_thread(runtime_context.close), name="runtime_manager.close")
        await close_task

        logger.debug(f"Exited {runtime_context} context.")
