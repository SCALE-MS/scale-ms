"""Workflow subpackage for local ScaleMS execution.

Execute subprocesses and functions in terms of the built-in asyncio framework.
Supports deferred execution by default, but close integration with the built-in
asyncio allows sensible behavior of concurrency constructs.

Example:
    python3 -m scalems.local my_workflow.py

"""

import asyncio
import importlib
import logging
import os
import pathlib
import weakref

from . import operations
from .. import execution as _execution
from .. import workflow as _workflow
from ..store import FileStore
from ..context import scoped_chdir
from ..exceptions import InternalError
from ..exceptions import MissingImplementationError
from ..exceptions import ProtocolError
from ..identifiers import TypeIdentifier
from ..subprocess._subprocess import SubprocessTask
from ..invocation import make_parser as _make_parser

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


parser = _make_parser(__package__)


def workflow_manager(loop: asyncio.AbstractEventLoop):
    """Standard basic workflow context for local execution.

    Uses the asyncio module to allow commands to be staged as asyncio coroutines.

    There is no implicit OS level multithreading or multiprocessing.
    """
    return _workflow.WorkflowManager(loop=loop, executor_factory=executor_factory)


class _ExecutionContext:
    """Represent the run time environment for a managed workflow item."""

    def __init__(self, manager: _workflow.WorkflowManager, identifier: bytes):
        if manager is None:
            raise ProtocolError("Could not acquire reference to WorkflowManager. Stale reference?")
        self.workflow_manager: _workflow.WorkflowManager = manager
        self.identifier: bytes = identifier
        try:
            self.workflow_manager.item(self.identifier)
        except Exception as e:
            raise InternalError("Unable to access managed item.") from e
        self._task_directory = pathlib.Path(os.path.join(os.getcwd(), self.identifier.hex()))  #
        # TODO: Find a canonical way to get
        # the workflow directory.

    @property
    def task_directory(self) -> pathlib.Path:
        return self._task_directory

    def done(self, set_value: bool = None) -> bool:
        done_token = os.path.join(self.task_directory, "done")
        if set_value is not None:
            if set_value:
                pathlib.Path(done_token).touch(exist_ok=True)
            else:
                pathlib.Path(done_token).unlink(missing_ok=True)
        return os.path.exists(done_token)


class WorkflowUpdater(_execution.AbstractWorkflowUpdater):
    def __init__(self, runtime: _execution.RuntimeManager):
        self.executor = runtime

    async def submit(self, *, item: _workflow.Task) -> asyncio.Task:
        # TODO: Ensemble handling
        item_shape = item.description().shape()
        if len(item_shape) != 1 or item_shape[0] != 1:
            raise MissingImplementationError("Executor cannot handle multidimensional tasks yet.")

        task: asyncio.Task = await submit(item=item)
        return task


async def submit(item: _workflow.Task) -> asyncio.Task:
    key = item.uid()
    # Note that we could insert resource management here. We could create
    # tasks until we run out of free resources, then switch modes to awaiting
    # tasks until resources become available, then switch back to placing tasks.
    execution_context = _ExecutionContext(item.manager(), key)
    if execution_context.done():
        if isinstance(key, bytes):
            id = key.hex()
        else:
            id = str(key)
        logger.info(f"Skipping task that is already done. ({id})")
        # TODO: Update local status and results.
    else:
        assert not item.done()
        assert not item.manager().item(key).done()
        task_type: TypeIdentifier = item.description().type()
        task = asyncio.create_task(_execute_item(task_type=task_type, item=item, execution_context=execution_context))
        return task


# TODO: return an execution status object?
async def _execute_item(
    task_type: TypeIdentifier, item: _workflow.Task, execution_context: _ExecutionContext  # noqa: C901
):
    # TODO: Automatically resolve resource types.
    if task_type == TypeIdentifier(("scalems", "subprocess", "SubprocessTask")):
        task_type = SubprocessTask()

        # TODO: Use abstract input factory.
        logger.debug("Resolving input for {}".format(str(item)))
        input_type = task_type.input_type()
        input_record = input_type(**item.input)
        input_resources = operations.input_resource_scope(context=execution_context, task_input=input_record)

        # We need to provide a scope in which we guarantee the availability of resources,
        # such as temporary files provided for input, or other internally-generated
        # asyncio entities.
        async with input_resources as subprocess_input:
            logger.debug("Creating coroutine for {}".format(task_type.__class__.__name__))
            # TODO: Use abstract task factory.
            coroutine = operations.subprocessCoroutine(context=execution_context, signature=subprocess_input)
            logger.debug("Creating asyncio Task for {}".format(repr(coroutine)))
            awaitable = asyncio.create_task(coroutine)

            # TODO: Use abstract results handler.
            logger.debug("Waiting for task to complete.")
            result = await awaitable
            subprocess_exception = awaitable.exception()
            if subprocess_exception is not None:
                logger.exception("subprocess task raised exception {}".format(str(subprocess_exception)))
                raise subprocess_exception
            logger.debug("Setting result for {}".format(str(item)))
            item.set_result(result)
            # TODO: Need rigorous task state machine.
            execution_context.done(item.done())
    else:
        # TODO: Process pool?
        # The only way we can be sure that tasks will not detect and use the working
        # directory of the main script's interpreter process is to actually
        # change directories. However, there isn't a good way to do that with concurrency.
        # We can either block concurrent execution within the changed directory,
        # or we can do the directory changing in subprocesses.
        #
        # Here we use a non-asynchronous context manager to avoid yielding
        # to the asyncio event loop. However, if there are other threads in the
        # current process that release the GIL, this is not safe.
        try:
            task_dir = execution_context.task_directory
            if not task_dir.exists():
                task_dir.mkdir()
            logger.debug(f"Entering {task_dir}")
            # Note: This limits us to one task-per-process.
            # TODO: Use one (or more) subprocesses per task, setting the working
            #  directory.
            # The forking may be specialized to the task type, or we can default
            # to just always forking the Python interpreter.
            with scoped_chdir(task_dir):
                # TODO: Use implementation registry.
                # We should have already checked for this...
                namespace_end = len(task_type.scoped_name())
                implementation = None
                while namespace_end >= 1:
                    try:
                        module = ".".join(task_type.scoped_name()[:namespace_end])
                        logger.debug(f"Looking for importable module: {module}")
                        implementation = importlib.import_module(module)
                    except ImportError:
                        implementation = None
                    else:
                        break
                    # Try the parent namespace?
                    namespace_end -= 1
                if namespace_end < 1:
                    raise InternalError("Bad task_type_identifier.")
                logger.debug(f'Looking for "scalems_helper in {module}.')
                helper = getattr(implementation, "scalems_helper", None)
                if not callable(helper):
                    raise MissingImplementationError(
                        "Executor does not have an implementation for {}".format(str(task_type))
                    )
                else:
                    logger.debug("Helper found. Passing item to helper for execution.")
                    # Note: we need more of a hook for two-way interaction here.
                    try:
                        local_resources = execution_context.workflow_manager
                        result = helper(item, context=local_resources)
                        item.set_result(result)
                    except Exception as e:
                        raise e
                    execution_context.done(item.done())
        except Exception as e:
            logger.exception(f"Badly handled exception: {e}")
            raise e


def executor_factory(manager: _workflow.WorkflowManager, params=None):
    if params is not None:
        raise TypeError("This factory does not accept a Configuration object.")
    executor = LocalExecutor(
        editor_factory=weakref.WeakMethod(manager.edit_item),
        datastore=manager.datastore(),
        loop=manager.loop(),
        dispatcher_lock=manager._dispatcher_lock,
    )
    return executor


class LocalExecutor(_execution.RuntimeManager):
    """Client side manager for work dispatched locally."""

    def __init__(
        self,
        *,
        editor_factory=None,
        datastore: FileStore = None,
        loop: asyncio.AbstractEventLoop,
        configuration=None,
        dispatcher_lock=None,
    ):
        """Create a client side execution manager.

        Initialization and deinitialization occurs through
        the Python (async) context manager protocol.
        """
        super().__init__(
            editor_factory=editor_factory,
            datastore=datastore,
            loop=loop,
            configuration=configuration,
            dispatcher_lock=dispatcher_lock,
        )

    def updater(self) -> WorkflowUpdater:
        return WorkflowUpdater(runtime=self)

    async def runtime_startup(self) -> asyncio.Task:
        runner_started = asyncio.Event()
        runner_task = asyncio.create_task(_execution.manage_execution(executor=self, processing_state=runner_started))
        await runner_started.wait()
        return runner_task
