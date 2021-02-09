"""Entry point subpackage for ScaleMS execution dispatching to RADICAL Pilot.

Example:
    python -m scalems.radical myworkflow.py

Manage workflow context for RADICAL Pilot.

Dispatching through RADICAL Pilot is still evolving, and this
module may provide multiple disparate concepts.

Context Manager:
    RPContextManager provides a SCALE-MS workflow context and coordinates
    resources for a RADICAL Pilot Session. When "entered" (i.e. used as
    a :py:func:`with`), the Python Context Manager protocol manages the
    lifetime of a radical.pilot.Session. Two significant areas of future
    development include Context chaining, and improved support for multiple rp.Sessions
    through multiple RPContextManager instances.

Executor:
    RPExecutor is an attempt to extend concurrent.futures.Executor.
    Unlike concurrent.futures.Executor, RPExecutor is not designed
    to accept arbitrary Python functions through *submit*. Also,
    the executor is not intended to be instantiated directly by users,
    but should be obtained through a RPContext instance in a scoped
    block. This allows better lifetime management of the executor
    and of the associated rp.Session.

Event loop:
    The concurrent.futures.Executor multi-tasking model may not be a useful
    abstraction for RADICAL Pilot. Alternatives include interacting
    more directly with the core asyncio functionality. To support such
    a model, "thread" contextual details for RPContextManager are stored
    as contextvars.ContextVar module variables, as appropriate.
    Module functions allow RP tasks to be expressed as Python awaitables,
    which use the asynchronous context manager protocol to manage
    rp.Session lifecycle in a coroutine function that is used to produce
    an awaitable workflow object.

"""
# TODO: Consider converting to a namespace package to improve modularity of implementation.


import asyncio
import concurrent.futures
import logging
import os
import warnings
import weakref
from concurrent.futures import Future
from types import TracebackType
from typing import Any, Callable, Optional, Tuple

import scalems.context
from scalems.exceptions import DispatchError, DuplicateKeyError, MissingImplementationError

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


class RPWorkflowContext(scalems.context.AbstractWorkflowContext):
    """Manage a workflow context for RADICAL Pilot work loads.

    The rp.Session is created when the Python Context Manager is "entered",
    so the asyncio event loop must be running before then.

    To help enforce this, we use an async Context Manager, at least in the
    initial implementation. However, the implementation is not thread-safe.
    It is not reentrant, but this is not checked. We probably _do_ want to
    constrain ourselves to zero or one Sessions per environment, but we _do_
    need to support multiple Pilots and task submission scopes (resource
    requirement groups).
    Further discussion is welcome.

    Warning:
        The importer of this module should be sure to import radical.pilot
        before importing the built-in logging module to avoid spurious warnings.

    TODO: Separate the WorkflowContext and its rp.Session management from the
          executor and its umgr management.
    """
    def __init__(self):
        # Import locally so that radical.pilot is only a dependency when used.
        import radical.pilot as rp

        # TODO: Eliminate use cases that require this exposure.
        self.rp = rp

        self.__rp_cfg = dict()
        if not 'RADICAL_PILOT_DBURL' in os.environ:
            raise DispatchError('RADICAL Pilot environment is not available.')

        resource = 'local.localhost'
        # TODO: Find default config?
        resource_config = {resource: {}}
        # TODO: Get from user or local config files.
        resource_config[resource].update({
            'project': None,
            'queue': None,
            'schema': None,
            'cores': 32,
            'gpus': 0
        })
        pilot_description = dict(resource=resource,
                                 runtime=30,
                                 exit_on_error=True,
                                 project=resource_config[resource]['project'],
                                 queue=resource_config[resource]['queue'],
                                 cores=resource_config[resource]['cores'],
                                 gpus=resource_config[resource]['gpus'])
        self.resource_config = resource_config
        self.pilot_description = pilot_description
        self.session = None
        self._finalizer = None
        self.umgr = None

        # Basic Context implementation details
        self.task_map = dict()  # Map UIDs to task Futures.
        self.contextvar_tokens = []
        self.event_loop = None

    def active(self) -> bool:
        session = self.session
        if session is None:
            return False
        else:
            assert session is not None
            return not session.closed

    # def submit(self, task_description: dict) -> Future:
    def add_task(self, task_description):
        """Placeholder for task creation interface.

        TODO: Subscribe to Futures in the task input.
        TODO: Dispatch task configuration according to registered implementations.
        TODO: Own a task instance and return a task view.
        TODO: Accept object types other than Subprocess (e.g. Data, PyFunc, or opaque dispatchable types).
        """
        from . import operations
        # TODO: more complete type hinting.
        if not isinstance(task_description, scalems.subprocess.Subprocess):
            raise MissingImplementationError('Operation not supported.')
        uid = task_description.uid()
        if uid in self.task_map:
            # TODO: Consider decreasing error level to `warning`.
            raise DuplicateKeyError('Task already present in workflow.')

        task = operations.executable(self, task_description)

        self.task_map[uid] = task
        return task

    async def run(self, task=None):
        """Run the configured workflow.

        TODO:
            Consider whether to use an awaitable argument as a hint to narrow the scope
            of the work graph to execute, or whether to just run everything.

        TODO: Move this function implementation to the executor instance / Session implementation.
        """
        if task is not None:
            raise MissingImplementationError('Semantics for run(task) are not yet defined.')
        # Bypass the need for asyncio.run()
        # if self.event_loop is None:
        #     raise RuntimeError('No event loop!')
        # loop = self.event_loop
        return await asyncio.wait(self.task_map.values())

    def shutdown(self):
        if self.active():
            self.session.close()
            assert self.session.closed
            # Is there any reason to reuse a closed Session?
            self.session = None
        else:
            warnings.warn('shutdown has been called more than once.')

    def __del__(self):
        if self.active():
            warnings.warn('{} was not explicitly shutdown.'.format(repr(self)))

    def __aenter__(self):
        # Save context module state since we are not using contextvars.Context.run() or equivalent.
        self.contextvar_tokens.append(scalems.context.parent.set(scalems.context.current.get()))
        self.contextvar_tokens.append(scalems.context.current.set(self))
        assert scalems.context.get_context() is self
        # context manager is not reentrant.
        assert self.session is None

        async def launch(context):
            context.session = context.rp.Session()
            pmgr = context.rp.PilotManager(session=context.session)
            context.umgr = context.rp.UnitManager(session=context.session)
            pilot = pmgr.submit_pilots(context.rp.ComputePilotDescription(context.pilot_description))
            context.umgr.add_pilots(pilot)
            # Note: We should have an active session now, ready to receive tasks, but
            # no tasks have been submitted.
            return context

        # Note that any return value from __aenter__() will be awaited.
        return launch(self)

    def __aexit__(self, exc_type, exc_val, exc_tb):
        async def finalize(exception_handler):
            self.shutdown()
            assert scalems.context.get_context() is self
            # Restore context module state since we are not using contextvars.Context.run() or equivalent.
            # TODO: We should either check that we have not branched/re-entered, or this scope should be captured as a single awaitable and contextvars.run().
            for token in self.contextvar_tokens:
                token.var.reset(token)
            # Note: This is a chance to await unawaited tasks and make extra-sure that RP is properly cleaned up.
            return exception_handler(exc_type, exc_val, exc_tb)

        return finalize(super().__exit__)


class RPResult:
    """Basic result type for RADICAL Pilot tasks.

    Define a return type for Futures or awaitable tasks from
    RADICAL Pilot commands.
    """


class RPFuture(concurrent.futures.Future):
    """Future interface for RADICAL Pilot tasks."""

    def __init__(self, task: weakref.ref) -> None:
        # Import locally so that radical.pilot is only a dependency when used.
        import radical.pilot as rp
        super().__init__()
        if not callable(task) or not isinstance(task(), rp.ComputeUnit):
            raise TypeError('Provide a callable that produces the rp ComputeUnit.')
        self.task = task

    def cancel(self) -> bool:
        raise MissingImplementationError()

    def cancelled(self) -> bool:
        return super().cancelled()

    def running(self) -> bool:
        raise MissingImplementationError()

    def add_done_callback(self, fn: Callable[[Future], Any]) -> None:
        # TODO: more complete type hinting.
        raise MissingImplementationError()

    def result(self, timeout: Optional[float] = ...) -> RPResult:
        if not self.done():
            # Note that task.wait() seems not to work reliably.
            # TODO: task.umgr.wait_units(uids=taskid)
            # Warning: Waiting on all units will deadlock in non-trivial cases.
            task = self.task()
            task.umgr.wait_units(uids=task.uid, timeout=timeout)
        return super().result()

    def set_running_or_notify_cancel(self) -> bool:
        raise MissingImplementationError()

    def exception(self, timeout: Optional[float] = ...) -> Optional[BaseException]:
        raise MissingImplementationError()

    def set_exception(self, exception: Optional[BaseException]) -> None:
        super().set_exception(exception)

    def exception_info(self, timeout: Optional[float] = ...) -> Tuple[Any, Optional[TracebackType]]:
        return super().exception_info(timeout)

    def set_exception_info(self, exception: Any, traceback: Optional[TracebackType]) -> None:
        super().set_exception_info(exception, traceback)

#
# class RPExecutor(concurrent.futures.Executor):
#     def __init__(self):
#         import radical.pilot as rp
#         self.rp = rp
#         self.__rp_cfg = dict()
#         if not 'RADICAL_PILOT_DBURL' in os.environ:
#             raise RuntimeError('RADICAL Pilot environment is not available.')
#
#         resource = 'local.localhost'
#         # TODO: Find default config?
#         resource_config = {resource: {}}
#         resource_config[resource].update({
#             'project': None,
#             'queue': None,
#             'schema': None,
#             'cores': 1,
#             'gpus': 0
#         })
#         pilot_description = dict(resource=resource,
#                                  runtime=30,
#                                  exit_on_error=True,
#                                  project=resource_config[resource]['project'],
#                                  queue=resource_config[resource]['queue'],
#                                  cores=resource_config[resource]['cores'],
#                                  gpus=resource_config[resource]['gpus'])
#         self.resource_config = resource_config
#         self.pilot_description = pilot_description
#         self.session = None
#         self._finalizer = None
#         self.umgr = None
#
#         # TODO: Integrate with event loop scoping to make sure the following occur in a contextvars.Context.
#         parent = scalems.context.current.get()
#         scalems.context.parent.set(parent)
#         scalems.context.current.set(self)
#         # TODO: Couple *active* to the status of self.session?
#         self.__active = True
#
#     def active(self) -> bool:
#         if self.session is None:
#             return False
#         else:
#             return not self.session.closed()
#
#     def submit(self, task_description: dict) -> Future:
#         # TODO: more complete type hinting.
#         # TODO: The `submit` signature should only apply to Python functions.
#         task = self.umgr.submit_units(
#             self.rp.ComputeUnitDescription(task_description))  # radical.pilot.ComputeUnit
#         # task.wait() just hangs. Using umgr.wait_units() instead...
#         # task.wait()
#         task_ref = weakref.ref(task)
#         future = RPFuture(task_ref)
#         def cb(obj, state):
#             # Where is the state enumeration?
#             # TODO: assert state in [...]
#             if task_ref().exit_code is not None:
#                 future.set_result(RPResult())
#         task.register_callback(cb)
#         return future
#
#     def shutdown(self):
#         if self.active():
#             context = scalems.context.current.get()
#             # TODO: Use contextvars to localize state data.
#             if context is not self:
#                 warnings.warn('Bad shutdown protocol may indicate race condition or leak: RPDispatcher is active, but not current.')
#             else:
#                 # TODO: Maintain context hierarchy...
#                 scalems.context.current.set(scalems.context.parent.get())
#             self.session.close()
#             assert self.session.closed()
#         else:
#             warnings.warn('shutdown has been called more than once.')
#
#     def __del__(self):
#         if self.active():
#             warnings.warn('RPDispatcher was not explicitly shutdown.')
#
#     def __enter__(self):
#         assert scalems.context.get_context() is self
#         assert self.session is None
#         session = self.rp.Session()
#         self._finalizer = weakref.finalize(self, session.close)
#         self.session = session
#         pmgr = self.rp.PilotManager(session=self.session)
#         self.umgr = self.rp.UnitManager(session=self.session)
#         pilot = pmgr.submit_pilots(self.rp.ComputePilotDescription(self.pilot_description))
#         self.umgr.add_pilots(pilot)
#         # Note: We should have an active session now, ready to receive tasks, but
#         # no tasks have been submitted.
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         # self.umgr.wait_units()
#         self.shutdown()
#         # Return False to indicate we have not handled any exceptions.
#         return False
