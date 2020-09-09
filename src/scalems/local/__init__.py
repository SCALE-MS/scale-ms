"""Workflow subpackage for local ScaleMS execution.

Execute subprocesses and functions in terms of the built-in asyncio framework.
Supports deferred execution by default, but close integration with the built-in
asyncio allows sensible behavior of concurrency constructs.

Example:
    python3 -m scalems.local my_workflow.py

"""
# TODO: Consider converting to a namespace package to improve modularity of implementation.


import asyncio
import concurrent.futures
import logging
import warnings
from typing import Any, Callable

import scalems.context
from scalems.exceptions import DuplicateKeyError, MissingImplementationError

from . import operations

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


class AsyncWorkflowContext(scalems.context.AbstractWorkflowContext):
    """Standard basic workflow context for local execution.

    Uses the asyncio module to allow commands to be staged as asyncio coroutines.

    There is no implicit OS level multithreading or multiprocessing.
    """
    def __init__(self):
        from asyncio import subprocess
        self.PIPE = getattr(subprocess, 'PIPE')
        self.STDOUT = getattr(subprocess, 'STDOUT')
        self.DEVNULL = getattr(subprocess, 'DEVNULL')
        self.subprocess = subprocess
        # Basic Context implementation details
        self.task_map = dict()  # Map UIDs to task Futures.
        self.contextvar_tokens = []
        self.event_loop = None

    def __enter__(self):
        # TODO: Use generated or base class behavior for managing the global context state.
        # TODO: Consider requiring the script to be wrapped in scalems.run() and/or to have a clear `main` function.
        # Save context module state since we are not using contextvars.Context.run() or equivalent.
        self.contextvar_tokens.append(scalems.context.parent.set(scalems.context.current.get()))
        self.contextvar_tokens.append(scalems.context.current.set(self))
        # Acquire event loop, creating one if necessary.
        # self.event_loop = asyncio.get_event_loop()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        loop = self.event_loop
        # if loop is None:
        #     raise RuntimeError('We should not be able to reach this point...')
        # if loop.is_running():
        #     # Clean up unawaited tasks.
        #     loop.run_until_complete(loop.shutdown_asyncgens())
        #     # Do we need to check the work graph directly?
        #     # We need to make sure the loop is not running before calling close()
        #     loop.stop()
        # Do we want to close the event loop here, as part of scalems.run(), or somewhere else?
        # loop.close()
        self.event_loop = None
        # Restore context module state since we are not using contextvars.Context.run() or equivalent.
        for token in self.contextvar_tokens:
            token.var.reset(token)
        return super().__exit__(exc_type, exc_val, exc_tb)

    # def add_task(self, operation: str, bound_input):
    def add_task(self, task_description):
        # # TODO: Resolve implementation details for *operation*.
        # if operation != 'scalems.executable':
        #     raise MissingImplementationError('No implementation for {} in {}'.format(operation, repr(self)))
        # # Copy a static copy of the input.
        # # TODO: Dispatch tasks addition, allowing negotiation of Context capabilities and subscription
        # #  to resources owned by other Contexts.
        # if not isinstance(bound_input, scalems.subprocess.SubprocessInput):
        #     raise ValueError('Only scalems.subprocess.SubprocessInput objects supported as input.')
        if not isinstance(task_description, scalems.subprocess.Subprocess):
            raise MissingImplementationError('Operation not supported.')
        uid = task_description.uid()
        if uid in self.task_map:
            # TODO: Consider decreasing error level to `warning`.
            raise DuplicateKeyError('Task already present in workflow.')
        # TODO: use generic reference to implementation.
        # TODO: DO NOT hold a reference to the client-provided object; CREATE a task in the current context.
        #       Make sure there are no artifacts of shallow copies that may result in a user modifying nested objects unexpectedly.
        awaitable = operations.executable(context=self, task=task_description)
        self.task_map[uid] = awaitable
        # TODO: Create (asyncio) task. Note that the event loop may already be running.
        # TODO: Return a real TaskView
        return uid

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

    def wait(self, awaitable, **kwargs):
        # TODO: We have to confirm that an event loop is running and properly handle awaitables.
        assert asyncio.iscoroutine(awaitable)
        raise MissingImplementationError()


# class LocalExecutor(concurrent.futures.Executor):
#     """Perform local execution in terms of the asyncio module."""
#     @staticmethod
#     async def wrap_function(fn: Callable, *args: Any, **kwargs: Any):
#         # TODO: resolve Futures in arguments.
#         # TODO: more complete type hinting for Callable signature.
#         assert callable(fn)
#         return fn(*args, **kwargs)
#
#     def submit(self, fn: Callable, *args: Any, **kwargs: Any):
#         """Overrides concurrent.futures.Executor.
#
#         Create a Task by scheduling *fn* to be executed with the provided arguments.
#
#         Note that this implementation is not thread-safe and needs to be
#         reconsidered. Currently it provides experimental internal implementation support.
#         """
#         assert callable(fn)
#         if asyncio.iscoroutinefunction(fn):
#             task = fn(*args, **kwargs)
#             assert asyncio.iscoroutine(task)
#         else:
#             raise MissingImplementationError('todo...')
#         return task
#
#     def __init__(self):
#         # TODO: Integrate with event loop scoping to make sure the following occur in a contextvars.Context.
#         parent = scalems.context.current.get()
#         scalems.context.parent.set(parent)
#         scalems.context.current.set(self)
#         self.__active = True
#
#     def shutdown(self, **kwargs):
#         """Extends concurrent.futures.Executor.
#         :param **kwargs: passed to base class.
#         """
#         if self.__active:
#             context = scalems.context.current.get()
#             if context is not self:
#                 # TODO: Use contextvars to localize state data.
#                 warnings.warn('Bad shut down protocol may indicate race condition or leak: LocalExecutor is active, but not current.')
#             else:
#                 # TODO: Maintain context hierarchy...
#                 scalems.context.current.set(scalems.context.parent.get())
#             self.__active = False
#         else:
#             warnings.warn('LocalExecutor.shutdown has been called more than once.')
#         return super().shutdown(**kwargs)
#
#     def __del__(self):
#         if self.__active:
#             warnings.warn('LocalExecutor was not explicitly shut down.')
#             self.shutdown()
#
#     def __enter__(self):
#         """Extends concurrent.futures.Executor"""
#         assert scalems.context.get_context() is self
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         """Extends concurrent.futures.Executor"""
#         self.shutdown()
#         # Return False to indicate we have not handled any exceptions.
#         return False
