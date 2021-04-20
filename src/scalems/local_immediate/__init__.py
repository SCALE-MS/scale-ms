"""Workflow context subpackage for immediate local ScaleMS execution.

Generally, local execution is best managed with the scalems.local module instead.
The WorkflowContext provided by this package combines workflow management with
immediate execution, which is not normative for ScaleMS execution,
but potentially useful for debugging.

Example:
    python3 -m scalems.local_immediate my_workflow.py

"""

import scalems.context
from scalems.context import ItemView
from scalems.exceptions import DuplicateKeyError
from scalems.exceptions import MissingImplementationError
from . import operations


class ImmediateExecutionContext(scalems.context.WorkflowManager):
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

    def item(self, identifier) -> ItemView:
        """Interact with a managed item.

        In the initial implementation, this supports both client ItemViews and
        executor updates, which may not be appropriate.
        """
        # Consider providing the consumer context when acquiring access.
        # Consider limiting the scope of access requested.
        return self.task_map[identifier]

    # def add_task(self, operation: str, bound_input):
    def add_item(self, task_description):
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
        self.task_map[uid] = operations.executable(context=self, task=task_description)
        # TODO: The return value should be a full proxy to a command instance.
        return uid

    def run(self, task):
        # If task belongs to this context, it has already run: no-op.
        return self.task_map[task]
