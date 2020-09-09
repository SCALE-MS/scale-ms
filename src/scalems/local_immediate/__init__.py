"""Workflow context subpackage for immediate local ScaleMS execution.

Generally, local execution is best managed with the scalems.local module instead.
The WorkflowContext provided by this package combines workflow management with
immediate execution, which is not normative for ScaleMS execution,
but potentially useful for debugging.

Example:
    python3 -m scalems.local_immediate my_workflow.py

"""


import asyncio
import concurrent.futures
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
        self.task_map[uid] = operations.executable(context=self, task=task_description)
        # TODO: The return value should be a full proxy to a command instance.
        return uid

    def run(self, task):
        # If task belongs to this context, it has already run: no-op.
        return self.task_map[task]

    def wait(self, awaitable):
        # Warning: this is not the right way to confirm the object does not need await...
        assert not asyncio.iscoroutine(awaitable)
        raise NotImplementedError()
