"""Entry point subpackage for local ScaleMS execution.

Example:
    python3 -m scalems.local my_workflow.py

"""
# TODO: Consider converting to a namespace package to improve modularity of implementation.


import asyncio
import concurrent.futures
import importlib
import warnings
from typing import Any, Callable

import scalems.context
from . import operations


class ImmediateExecutionContext(scalems.context.AbstractWorkflowContext):
    """Workflow context for immediately executed commands.

    Commands are executed immediately upon addition to the work flow. Data flow
    must be resolvable at command instantiation.

    Intended for debugging.
    """

    def __init__(self):
        # Details for scalems.subprocess module compatibility.
        import subprocess
        self.PIPE = getattr(subprocess, 'PIPE')
        self.STDOUT = getattr(subprocess, 'STDOUT')
        self.DEVNULL = getattr(subprocess, 'DEVNULL')
        self.subprocess = subprocess
        # Basic Context implementation details
        self.task_map = dict()  # Map UIDs to task Futures.
        self.contextvar_tokens = []

    def subprocess_runner(self, *args, **kwargs):
        p = self.subprocess.Popen(*args, **kwargs)
        p.communicate()

    def __enter__(self):
        # TODO: Use generated or base class behavior for managing the global context state.
        # TODO: Consider using the asyncio event loop for ImmediateExecution (and all contexts).
        self.contextvar_tokens.append(scalems.context.parent.set(scalems.context.current.get()))
        self.contextvar_tokens.append(scalems.context.current.set(self))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for token in self.contextvar_tokens:
            token.var.reset(token)
        return super().__exit__(exc_type, exc_val, exc_tb)

    # def add_task(self, operation: str, bound_input):
    def add_task(self, task_description):
        # # TODO: Resolve implementation details for *operation*.
        # if operation != 'scalems.executable':
        #     raise NotImplementedError('No implementation for {} in {}'.format(operation, repr(self)))
        # # Copy a static copy of the input.
        # # TODO: Dispatch tasks addition, allowing negotiation of Context capabilities and subscription
        # #  to resources owned by other Contexts.
        # if not isinstance(bound_input, scalems.subprocess.SubprocessInput):
        #     raise ValueError('Only scalems.subprocess.SubprocessInput objects supported as input.')
        if not isinstance(task_description, scalems.subprocess.Subprocess):
            raise NotImplementedError('Operation not supported.')
        uid = task_description.uid()
        if uid in self.task_map:
            # TODO: Consider decreasing error level to `warning`.
            raise ValueError('Task already present in workflow.')
        # TODO: use generic reference to implementation.
        self.task_map[uid] = operations.executable(task_description)
        return uid

    def run(self, task):
        # If task belongs to this context, it has already run: no-op.
        return self.task_map[task]

    def wait(self, awaitable):
        # Warning: this is not the right way to confirm the object does not need await...
        assert not asyncio.iscoroutine(awaitable)
        raise NotImplementedError()


class AsyncExecutionContext(scalems.context.AbstractWorkflowContext):
    """Standard basic workflow context for local execution.

    Uses the asyncio module to allow commands to be staged as asyncio coroutines.

    There is no implicit OS level multithreading or multiprocessing.
    """
    asyncio = asyncio

    def __init__(self):
        subprocess = importlib.import_module('subprocess', self.asyncio)
        self.PIPE = getattr(subprocess, 'PIPE')
        self.STDOUT = getattr(subprocess, 'STDOUT')
        self.DEVNULL = getattr(subprocess, 'DEVNULL')
        self.subprocess = subprocess

    def run(self, task):
        # Call asyncio.run()
        ...

    def wait(self, awaitable, **kwargs):
        # TODO: We have to confirm that an event loop is running and properly handle awaitables.
        assert asyncio.iscoroutine(awaitable)
        raise NotImplementedError()


class LocalExecutor(concurrent.futures.Executor):
    """Perform local execution in terms of the asyncio module."""
    @staticmethod
    async def wrap_function(fn: Callable, *args: Any, **kwargs: Any):
        # TODO: resolve Futures in arguments.
        # TODO: more complete type hinting for Callable signature.
        assert callable(fn)
        return fn(*args, **kwargs)

    def submit(self, fn: Callable, *args: Any, **kwargs: Any):
        """Overrides concurrent.futures.Executor.

        Create a Task by scheduling *fn* to be executed with the provided arguments.

        Note that this implementation is not thread-safe and needs to be
        reconsidered. Currently it provides experimental internal implementation support.
        """
        assert callable(fn)
        if asyncio.iscoroutinefunction(fn):
            task = fn(*args, **kwargs)
            assert asyncio.iscoroutine(task)
        else:
            raise NotImplementedError('todo...')
        return task

    def __init__(self):
        # TODO: Integrate with event loop scoping to make sure the following occur in a contextvars.Context.
        parent = scalems.context.current.get()
        scalems.context.parent.set(parent)
        scalems.context.current.set(self)
        self.__active = True

    def shutdown(self, **kwargs):
        """Extends concurrent.futures.Executor.
        :param **kwargs: passed to base class.
        """
        if self.__active:
            context = scalems.context.current.get()
            if context is not self:
                # TODO: Use contextvars to localize state data.
                warnings.warn('Bad shut down protocol may indicate race condition or leak: LocalExecutor is active, but not current.')
            else:
                # TODO: Maintain context hierarchy...
                scalems.context.current.set(scalems.context.parent.get())
            self.__active = False
        else:
            warnings.warn('LocalExecutor.shutdown has been called more than once.')
        return super().shutdown(**kwargs)

    def __del__(self):
        if self.__active:
            warnings.warn('LocalExecutor was not explicitly shut down.')
            self.shutdown()

    def __enter__(self):
        """Extends concurrent.futures.Executor"""
        assert scalems.context.get_context() is self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Extends concurrent.futures.Executor"""
        self.shutdown()
        # Return False to indicate we have not handled any exceptions.
        return False
