"""Manage the RADICAL Pilot start-up and shut-down.

The provided Runtime class encapsulates stateful resources that, once acquired,
should be shut down explicitly. Runtime instances may be used as context managers to
ensure the proper protocol is followed, or the caller can take responsibility for
calling Runtime.close() to shut down.

Note: Consider whether Runtime context manager is reentrant or multi-use.

The Runtime state encapsulates several nested states. A Session, TaskManager,
and PilotManager must be created for the Runtime to be usable. Additionally, Pilots and
scheduler tasks may be added or removed during the Runtime lifetime. To better support
the alternate scenarios when a Runtime instance may be provided to a scalems.radical
component in an arbitrary state, consider making the ``with`` block scoped, such that
it only returns the Runtime instance to its initial state when exiting, rather than
shutting down completely.
See also https://docs.python.org/3/library/contextlib.html#contextlib.ExitStack

Deferred:
    Runtime can avoid providing direct access to RP interface, and instead run an
    entire RP Session state machine in a thread (separate from the asyncio event loop
    thread), relaying RP scripting commands through queues, in order to completely
    prevent misuse and to insulate the asyncio event loop from blocking RP commands.
    We need to get a better sense of the RP flow combinatorics before we can reasonably
    pursue this.

See Also:
    https://github.com/SCALE-MS/scale-ms/issues/55

.. uml::

    title scalems on radical.pilot run time

    box "SCALE-MS framework" #honeydew
    participant WorkflowManager as client_workflowmanager
    participant RuntimeManager
    participant "runner task" as scalems.execution
    end box
    box "SCALE-MS RP adapter" #linen
    participant RuntimeSession as client_runtime
    participant Executor as client_executor
    end box

    autoactivate on

    client_workflowmanager -> client_workflowmanager: async with executor
    client_workflowmanager -> client_executor: ~__aenter__()
    activate client_workflowmanager
    client_executor -> RuntimeManager: ~__aenter__() base class method
    RuntimeManager -> client_executor: runtime_configuration()
    return
    RuntimeManager -> client_executor: runtime_startup()

    client_executor -> : rp.Session()
    return
    client_executor -> client_runtime **: Session
    activate client_runtime
    client_executor -> : rp.PilotManager()
    return
    client_executor -> client_runtime: pilot_manager()
    return
    client_executor -> : rp.TaskManager()
    return
    client_executor -> client_runtime: task_manager()
    return
    client_executor -> : pilot_manager.submit_pilots()
    return
    client_executor -> client_runtime: pilot()
    note left
    Pilot venv is determined by resource definition (JSON file).
    end note
    return

    group ref [scalems.radical.raptor]
    client_executor -> client_executor: get_raptor()
    return
    end

    client_executor -> client_runtime: set raptor
    return

    client_executor ->> scalems.execution **: create_task(manage_execution)
    client_executor -> scalems.execution: await runner_started
    RuntimeManager <-- client_executor: asyncio.Task
    RuntimeManager -> RuntimeManager: set runner_task
    return
    RuntimeManager --> client_executor: self
    client_workflowmanager <-- client_executor: RuntimeManager
    client_workflowmanager --> client_workflowmanager: as manager
    deactivate RuntimeManager

    client_workflowmanager -> client_workflowmanager #gray: async with dispatcher

    ...Raptor workload handling...

    return leave dispatcher context

    == Shut down runtime ==

    client_workflowmanager -> client_executor: ~__aexit__()
    client_executor -> RuntimeManager: ~__aexit__() base class method
    RuntimeManager ->> scalems.execution: enqueue a stop control message
    deactivate scalems.execution
    RuntimeManager -> RuntimeManager: await runner_task
    note right
        drain the queue
    end note
    RuntimeManager <-- scalems.execution
    deactivate RuntimeManager
    RuntimeManager ->> client_executor: runtime_shutdown()

    client_runtime <- client_executor
    return session
    client_runtime <- client_executor
    return raptor

    group Finalize Raptor task [if raptor is not None]
        group Give Raptor some time to shut down [if raptor.state not in FINAL]
            client_executor -> : runtime.raptor.wait(timeout=60)
        end
        group Forcibly cancel Raptor task [if raptor.state not in FINAL]
            client_runtime <- client_executor
            return task_manager
            client_executor -> : task_manager.cancel_tasks()
            return
            client_runtime <- client_executor
            return raptor
            client_executor -> : runtime.raptor.wait()
            return
        end
    end

    client_executor -> : session.close()
    return

    RuntimeManager -> RuntimeManager: await runtime_shutdown()

    client_executor -> client_runtime !!
    RuntimeManager <<-- client_executor
    RuntimeManager --> RuntimeManager
    RuntimeManager --> client_executor

    client_workflowmanager <-- client_executor: leave executor context
    client_workflowmanager --> client_workflowmanager

"""

from __future__ import annotations

__all__ = ("executor_factory", "current_configuration")

import asyncio
import contextlib
import contextvars
import logging
import os
import typing
import warnings
import weakref

from radical import pilot as rp

import scalems.call
import scalems.exceptions
import scalems.execution
import scalems.file
import scalems.invocation
import scalems.messages
import scalems.radical
import scalems.radical.runtime_configuration
import scalems.radical.raptor
import scalems.store
import scalems.subprocess
import scalems.workflow
from scalems.exceptions import DispatchError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from .runtime_configuration import get_pre_exec
from .runtime_configuration import RuntimeConfiguration
from .session import runtime_session
from .session import RuntimeSession
from .raptor import get_scheduler
from .task import submit
from ..store import FileStore
from ..execution import AbstractWorkflowUpdater
from ..identifiers import EphemeralIdentifier

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


_configuration = contextvars.ContextVar("_configuration")


def current_configuration() -> typing.Optional[RuntimeConfiguration]:
    """Get the current RADICAL runtime configuration, if any.

    Returns:
        RuntimeConfiguration | None: The configuration for the RuntimeManager scope,
            if any, or None.

    TODO: Don't use a global and globally accessible module state. Let context managers
        like RuntimeManager use their own ContextVars if appropriate.
    """
    return _configuration.get(None)


class RPDispatchingExecutor(scalems.execution.RuntimeManager[RuntimeConfiguration]):
    """Client side manager for work dispatched through RADICAL Pilot.

    Extends :py:class:`scalems.execution.RuntimeManager`

    Configuration points:

    * resource config
    * pilot config
    * session config?

    We try to wrap rp UI calls in separate threads. Note, though, that

    * The rp.Session needs to be created in the root thread to be able to correctly
      manage signal handlers and subprocesses, and
    * We need to be able to schedule RP Task callbacks in the same process as the
      asyncio event loop in order to handle Futures for RP tasks.

    See https://github.com/SCALE-MS/randowtal/issues/2
    """

    runtime: RuntimeSession
    """See `scalems.execution.RuntimeManager.runtime`"""

    def __init__(
        self,
        *,
        editor_factory: typing.Callable[[], typing.Callable] = None,
        datastore: FileStore = None,
        loop: asyncio.AbstractEventLoop,
        configuration: RuntimeConfiguration,
    ):
        """Create a client side execution manager.

        Warning:
            The creation method does not fully initialize the instance.

            Initialization and de-initialization occurs through
            the Python (async) context manager protocol.

        """
        if "RADICAL_PILOT_DBURL" not in os.environ:
            raise DispatchError("RADICAL Pilot environment is not available.")

        if not isinstance(configuration.target_venv, str) or len(configuration.target_venv) == 0:
            raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

        super().__init__(editor_factory=editor_factory, datastore=datastore, loop=loop, configuration=configuration)

    @contextlib.contextmanager
    def runtime_configuration(self):
        """Provide scoped Configuration.

        Merge the runtime manager's configuration with the global configuration,
        update the global configuration, and yield the configuration for a ``with`` block.

        Restores the previous global configuration when exiting the ``with`` block.

        Warning:
            Not thread-safe.

            We use the ContextVar to check for (and disallow) re-entrance.
            This contextmanager is not async, but it could be (and is) used within
            an asynchronous context manager. If used from multiple threads,
            we would not have anything structurally prohibiting reentrant calls.

        Design notes:
            Instance configuration is coupled to module state except as allowed
            through `contextvars.copy()`.
        """
        if _configuration.get(None):
            raise scalems.exceptions.APIError("A scalems.radical runtime is already active.")
        assert self.datastore is not None

        c = self.configuration()

        token = _configuration.set(c)
        try:
            yield c
        finally:
            _configuration.reset(token)

    async def runtime_startup(self) -> asyncio.Task:
        """Establish the RP Session.

        Acquire a maximally re-usable set of RP resources. The scope established by
        this function is as broad as it can be within the life of the workflow manager.

        Once *instance.runtime_startup()* succeeds, *instance.runtime_shutdown()*
        must be called to clean up resources.
        Use the async context manager behavior of the instance to
        automatically follow this protocol. I.e. instead of calling
        ``instance.runtime_startup(); ...; instance.runtime_shutdown()``,
        use::

            async with instance:
                ...

        Raises:
            DispatchError: if task dispatching could not be set up.
            asyncio.CancelledError: if parent `asyncio.Task` is cancelled while executing.

        Note:
            **Signal handling**

            RP is known to use IPC signals in several cases. We believe that the
            client environment should only experience an internally triggered
            SIGINT in a single code path, and the behavior can be suppressed by
            setting the Pilot's :py:attr:`~radical.pilot.PilotDescription.exit_on_error`
            attribute to `False`.

            We could use
            `loop.add_signal_handler()
            <https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.add_signal_handler>`__
            to convert to an exception that we can raise in an appropriate task, but this
            is probably unnecessary. Moreover, with Python 3.11, we get a sensible
            signal handling behavior (for SIGINT) with :py:class:`asyncio.Runner`.
            Per https://docs.python.org/3/library/asyncio-runner.html#handling-keyboard-interruption,
            we can just make sure that our run time resources will be properly
            shut down in the event of a :py:class:`asyncio.CancelledError`, including
            sending appropriate calls to the `radical.pilot` framework.

            See Also:
                https://github.com/SCALE-MS/randowtal/issues/1

        TODO: More concurrency.
            The rp.Pilot and raptor task can be separately awaited, and we should allow
            input data staging to begin as soon as we have enough run time details to do it.
            We need to clarify which tasks should be in which state to consider the
            asynchronous context manager to have been successfully "entered". I expect
            that success includes a Pilot in state PMGR_ACTIVE, and a raptor Task in
            state AGENT_EXECUTING, and asyncio.Task handles available for other aspects,
            like synchronization of metadata and initiation of input data staging.
            However, we may prefer that the workflow script can continue evaluation on
            the client side while waiting for the Pilot job, and that we avoid blocking
            until we absolutely have to (presumably when exiting the dispatching context).

        """
        config: RuntimeConfiguration = self._runtime_configuration

        # TODO: Check that we have a FileStore.

        try:
            #
            # Start the Session.
            #

            # Note: the current implementation implies that only one Task for the dispatcher
            # will exist at a time. We are further assuming that there will probably only
            # be one Task per the lifetime of the dispatcher object.
            # We could choose another approach and change our assumptions, if appropriate.
            logger.debug("Entering RP dispatching context. Waiting for rp.Session.")

            _runtime: RuntimeSession = await runtime_session(loop=self._loop, configuration=config)

            # TODO: Asynchronous data staging optimization.
            #  Allow input data staging to begin before scheduler is in state EXECUTING and
            #  before Pilot is in state PMGR_ACTIVE.

            assert _runtime.raptor is None

            # Get a scheduler task IFF raptor is explicitly enabled.
            if config.enable_raptor:
                task_manager = _runtime.task_manager()
                # Note that get_scheduler is a coroutine that, itself, returns a rp.Task.
                # We await the result of get_scheduler, then store the scheduler Task.
                _runtime.raptor = await asyncio.create_task(
                    get_scheduler(
                        pre_exec=list(get_pre_exec(config)),
                        task_manager=task_manager,
                        filestore=self.datastore,
                        scalems_env="scalems_venv",
                        # TODO: normalize ownership of this name.
                    ),
                    name="get-scheduler",
                )  # Note that we can derive scheduler_name from self.scheduler.uid in later methods.
        except asyncio.CancelledError as e:
            raise e
        except Exception as e:
            logger.exception("Exception while connecting RADICAL Pilot.", exc_info=e)
            raise DispatchError("Failed to launch SCALE-MS raptor task.") from e

        self.runtime = _runtime

        if self.runtime is None or self.runtime.session.closed:
            raise ProtocolError("Cannot process queue without a RP Session.")

        # Launch queue processor (proxy executor).
        # TODO: Make runtime_startup optional. Let it return a resource that is
        #  provided to the normalized run_executor(), or maybe use it to configure the
        #  Submitter that will be provided to the run_executor.
        runner_started = asyncio.Event()
        runner_task = asyncio.create_task(scalems.execution.manage_execution(self, processing_state=runner_started))
        await runner_started.wait()
        # TODO: Note the expected scope of the runner_task lifetime with respect to
        #  the global state changes (i.e. ContextVars and locks).
        return runner_task

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

    def runtime_shutdown(self, runtime: RuntimeSession):
        """Manage tear down of the RADICAL Pilot Session and resources.

        Several aspects of the RP runtime interface use blocking calls.
        This method should be run in a non-root thread (concurrent.futures.Future)
        that the event loop can manage as an asyncio-wrapped task.

        Overrides :py:class:`scalems.execution.RuntimeManager`
        """
        # TODO: Move this to a RuntimeSession.close() method.
        session: rp.Session = getattr(runtime, "session", None)
        if session is None:
            raise scalems.exceptions.APIError(f"No Session in {runtime}.")
        if session.closed:
            logger.error("RuntimeSession is already closed?!")
        else:
            if runtime.raptor is not None:
                # Note: The __aexit__ for the RuntimeManager makes sure that a `stop`
                # is issued after the work queue is drained, if the scheduler task has
                # not already ended. We could check the status of this stop message...
                if runtime.raptor.state not in rp.FINAL:
                    logger.info(f"Waiting for RP Raptor raptor task {runtime.raptor.uid} to complete...")
                    runtime.raptor.wait(rp.FINAL, timeout=60)
                if runtime.raptor.state not in rp.FINAL:
                    # Cancel the raptor.
                    logger.warning("Canceling the raptor scheduling task.")
                    # Note: the effect of CANCEL is to send SIGTERM to the shell that
                    # launched the task process.
                    # TODO: Report incomplete tasks and handle the consequences of terminating the
                    #  Raptor processes early.
                    task_manager = runtime.task_manager()
                    task_manager.cancel_tasks(uids=runtime.raptor.uid)
                # As of https://github.com/radical-cybertools/radical.pilot/pull/2702,
                # we do not expect `cancel` to block, so we must wait for the
                # cancellation to succeed. It shouldn't take long, but it is not
                # instantaneous or synchronous. We hope that a minute is enough.
                final_state = runtime.raptor.wait(state=rp.FINAL, timeout=10)
                logger.debug(f"Final state: {final_state}")
                logger.info(f"Master scheduling task state {runtime.raptor.state}: {repr(runtime.raptor)}.")
                if runtime.raptor.stdout:
                    # TODO(#229): Fetch actual stdout file.
                    logger.debug(runtime.raptor.stdout)
                if runtime.raptor.stderr:
                    # TODO(#229): Fetch actual stderr file.
                    # Note that all of the logging output goes to stderr, it seems,
                    # so we shouldn't necessarily consider it an error level event.
                    logger.debug(runtime.raptor.stderr)  # TODO(#108,#229): Receive report of work handled by Master.

            runtime.close()

            if session.closed:
                logger.debug(f"Session {session.uid} closed.")
            else:
                logger.error(f"Session {session.uid} not closed!")
        logger.debug("RuntimeSession shut down.")

    def updater(self) -> "WorkflowUpdater":
        return WorkflowUpdater(executor=self)


def executor_factory(manager: scalems.workflow.WorkflowManager, params: RuntimeConfiguration = None):
    if params is None:
        warnings.warn("executor_factory called without explicit configuration.")
        params = scalems.radical.runtime_configuration.configuration()
    elif not isinstance(params, RuntimeConfiguration):
        raise ValueError("scalems.radical executor_factory *params* must be a RuntimeConfiguration instance.")

    executor = RPDispatchingExecutor(
        editor_factory=weakref.WeakMethod(manager.edit_item),
        datastore=manager.datastore(),
        loop=manager.loop(),
        configuration=params,
    )
    return executor


class RPResult:
    """Basic result type for RADICAL Pilot tasks.

    Define a return type for Futures or awaitable tasks from
    RADICAL Pilot commands.
    """

    # TODO: Provide support for RP-specific versions of standard SCALEMS result types.


class WorkflowUpdater(AbstractWorkflowUpdater):
    def __init__(self, executor: RPDispatchingExecutor):
        self.executor = executor
        self.task_manager = executor.runtime.task_manager()
        # TODO: Make sure we are clear about the scope of the configuration and the
        #  life time of the workflow updater / submitter.
        self._pre_exec = list(get_pre_exec(executor.configuration()))

    async def submit(self, *, item: scalems.workflow.Task) -> asyncio.Task:
        # TODO: Ensemble handling
        item_shape = item.description().shape()
        if len(item_shape) != 1 or item_shape[0] != 1:
            raise MissingImplementationError("Executor cannot handle multidimensional tasks yet.")

        task: asyncio.Task[rp.Task] = await submit(item=item, task_manager=self.task_manager, pre_exec=self._pre_exec)
        return task
