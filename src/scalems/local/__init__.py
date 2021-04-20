"""Workflow subpackage for local ScaleMS execution.

Execute subprocesses and functions in terms of the built-in asyncio framework.
Supports deferred execution by default, but close integration with the built-in
asyncio allows sensible behavior of concurrency constructs.

Example:
    python3 -m scalems.local my_workflow.py

"""
# TODO: Consider converting to a namespace package to improve modularity of implementation.


import asyncio
import contextlib
import importlib
import logging
import os
import pathlib
import queue
import typing

import warnings

from .. import context as _context
from ..exceptions import InternalError
from ..exceptions import MissingImplementationError
from ..exceptions import ProtocolError
from ..subprocess._subprocess import SubprocessTask
from . import operations
from ..context import QueueItem

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


# TO DO NEXT: Implement the queue, dispatcher, executor (and internal queue)
# and, come on... add the fingerprinter... and the basic serializer...  `


class AsyncWorkflowManager(_context.WorkflowManager):
    """Standard basic workflow context for local execution.

    Uses the asyncio module to allow commands to be staged as asyncio coroutines.

    There is no implicit OS level multithreading or multiprocessing.
    """

    def __init__(self, loop):
        self._executor_factory = executor_factory
        super(AsyncWorkflowManager, self).__init__(loop)

    async def run(self, task=None, **kwargs):
        """Run the configured workflow.

        TODO:
            Consider whether to use an awaitable argument as a hint to narrow the scope
            of the work graph to execute, or whether to just run everything.

        TODO: Move this function implementation to the executor instance / Session implementation.
        """
        if len(kwargs) > 0:
            raise TypeError('One or more unrecognized key word arguments: {}'.format(', '.join(kwargs.keys())))

        logger.debug('{}.run() called on {}'.format(str(type(self).__name__), repr(self)))
        try:
            async with self.dispatch():
                logger.debug('Entered dispatch().')
                # if isinstance(task, ItemView):
                #     raise MissingImplementationError('Semantics for run(task) are not yet defined.')
                if task is None:
                    logger.debug('Running all work managed by {}.'.format(str(self)))
                    # The current queue will be processed by the activation of the dispatch() context manager.
                    return None
                if asyncio.iscoroutine(task):
                    logger.debug('Awaiting asyncio coroutine.')
                    result = await task
                elif asyncio.iscoroutinefunction(task):
                    logger.debug('Running coroutine with provided args.')
                    result = await task(**kwargs)
                elif callable(task):
                    logger.debug('Running provided function within asyncio event loop.')
                    result = task(**kwargs)
                else:
                    raise TypeError('Unrecognized awaitable: {}'.format(repr(task)))
                return result
        except Exception as e:
            logger.exception('Uncaught exception during {}.run(): {}'.format(type(self).__name__, str(e)))
        finally:
            # Note: The following line will not appear until the dispatch context manager has finished exiting.
            logger.debug('Leaving {}.run()'.format(type(self).__name__))


class _ExecutionContext:
    """Represent the run time environment for a managed workflow item."""

    def __init__(self, manager: _context.WorkflowManager, identifier: bytes):
        self.workflow_manager: _context.WorkflowManager = manager
        self.identifier: bytes = identifier
        try:
            self.workflow_manager.item(self.identifier)
        except Exception as e:
            raise InternalError('Unable to access managed item.') from e
        self._task_directory = pathlib.Path(os.path.join(os.getcwd(), self.identifier.hex()))
        # TODO: Find a canonical way to get the workflow directory.

    @property
    def task_directory(self) -> pathlib.Path:
        return self._task_directory

    def done(self, set_value: bool = None) -> bool:
        done_token = os.path.join(self.task_directory, 'done')
        if set_value is not None:
            if set_value:
                pathlib.Path(done_token).touch(exist_ok=True)
            else:
                pathlib.Path(done_token).unlink(missing_ok=True)
        return os.path.exists(done_token)


# TODO: Consider explicitly decoupling client-facing workflow manager from executor-facing manager.
async def run_executor(executor: 'LocalExecutor', *, processing_state: asyncio.Event, queue: asyncio.Queue):
    """Process workflow messages until a stop message is received.

    Initial implementation processes commands serially without regard for possible
    concurrency.

    Towards concurrency:
        We can create all tasks without awaiting any of them.

        Some tasks will be awaiting results from other tasks.

        All tasks will be awaiting a asyncio.Lock or asyncio.Condition for each
        required resource, but must do so indirectly.

        To avoid dead-locks, we can't have a Lock object for each resource unless
        they are managed by an intermediary that can do some serialization of requests.
        In other words, we need a Scheduler that tracks the resource pool, packages
        resource locks only when they can all be acquired without race conditions or blocking,
        and which then notifies the Condition for each task that it is allowed to run.

        It should not do so until the dependencies of the task are known to have
        all of the resources they need to complete (running with any dynamic dependencies
        also running) and, preferably, complete.

        Alternatively, the Scheduler can operate in blocks, allocating all resources,
        offering the locks to tasks, waiting for all resources to be released, then repeating.
        We can allow some conditions to "wake up" the scheduler to back fill a block
        of resources, but we should be careful with that.

        (We still need to consider dynamic tasks that
        generate other tasks. I think the only way to distinguish tasks which can't be
        dynamic from those which might be would be with the `def` versus `async def` in
        the implementing function declaration. If we abstract `await` with `scalems.wait`,
        we can throw an exception at execution time after checking a ContextVar.
        It may be better to just let implementers use `await` for dynamically created tasks,
        but we need to make the same check if a function calls `.result()` or otherwise
        tries to create a dependency on an item that was not allocated resources before
        the function started executing. In a conservative first draft, we can simply
        throw an exception if a non-`async def` function attempts to call a scalems workflow
        command like add_item while in an executing context.)

    """
    # Could also accept a "stop" Event object for the loop conditional,
    # but we would still need a way to yield on an empty queue until either
    # the "stop" event or an item arrives, and then we might have to account
    # for queues that potentially never yield any items, such as by sleeping briefly.
    # We should be able to do
    #     signal_task = asyncio.create_task(stop_event.wait())
    #     queue_getter = asyncio.create_task(command_queue.get())
    #     waiter = asyncio.create_task(asyncio.wait((signal_task, queue_getter), return_when=FIRST_COMPLETED))
    #     while await waiter:
    #         done, pending = waiter.result()
    #         assert len(done) == 1
    #         if signal_task in done:
    #             break
    #         else:
    #             command: QueueItem = queue_getter.result()
    #         ...
    #         queue_getter = asyncio.create_task(command_queue.get())
    #         waiter = asyncio.create_task(asyncio(wait((signal_task, queue_getter), return_when=FIRST_COMPLETED))
    #
    processing_state.set()
    while True:
        command: QueueItem = await queue.get()
        # Developer note: The preceding line and the following try/finally block are coupled!
        # Once we have awaited asyncio.Queue.get(), we _must_ have a corresponding
        # asyncio.Queue.task_done(). For tidiness, we immediately enter a `try` block with a
        # `finally` suite. Don't separate these without considering the Queue protocol.
        try:
            if not len(command.items()) == 1:
                raise ProtocolError('Expected a single key-value pair.')
            logger.debug('Executor is handling {}'.format(str(command)))

            # TODO: Use formal RPC protocol.
            if 'control' in command:
                if command['control'] == 'stop':
                    # This effectively breaks the `while True` loop, but may not be obvious.
                    # Consider explicit `break` to clarify that we want to run off the end
                    # of the function.
                    return
                else:
                    raise ProtocolError('Unknown command: {}'.format(command['control']))
            if 'add_item' not in command:
                raise MissingImplementationError('Executor has no implementation for {}'.format(str(command)))

            key = command['add_item']
            with executor.source_context.edit_item(key) as item:
                if not isinstance(item, _context.Task):
                    raise InternalError(
                        'Expected {}.item() to return a scalems.context.Task'.format(repr(executor.source_context)))

                # TODO: Ensemble handling
                item_shape = item.description().shape()
                if len(item_shape) != 1 or item_shape[0] != 1:
                    raise MissingImplementationError('Executor cannot handle multidimensional tasks yet.')

                task_type: _context.ResourceType = item.description().type()
                # Note that we could insert resource management here. We could create
                # tasks until we run out of free resources, then switch modes to awaiting
                # tasks until resources become available, then switch back to placing tasks.
                execution_context = _ExecutionContext(executor.source_context, key)
                if execution_context.done():
                    if isinstance(key, bytes):
                        id = key.hex()
                    else:
                        id = str(key)
                    logger.info(f'Skipping task that is already done. ({id})')
                    # TODO: Update local status and results.
                else:
                    assert not item.done()
                    assert not executor.source_context.item(key).done()
                    task = asyncio.create_task(_execute_item(task_type=task_type,
                                        item=item,
                                        execution_context=execution_context))
                    executor.submitted_tasks.append(task)
                # TODO: output handling
                # TODO: failure handling
        except Exception as e:
            logger.debug('Leaving queue runner due to exception.')
            raise e
        finally:
            # Warning: there is a tiny chance that we could receive a asyncio.CancelledError at this line
            # and fail to decrement the queue.
            logger.debug('Releasing "{}" from command queue.'.format(str(command)))
            queue.task_done()


# TODO: return an execution status object?
async def _execute_item(task_type: _context.ResourceType,
                        item: _context.Task,
                        execution_context: _ExecutionContext):
    # TODO: Automatically resolve resource types.
    if task_type.identifier() == 'scalems.subprocess.SubprocessTask':
        task_type = SubprocessTask()

        # TODO: Use abstract input factory.
        logger.debug('Resolving input for {}'.format(str(item)))
        input_type = task_type.input_type()
        input_record = input_type(**item.input)
        input_resources = operations.input_resource_scope(context=execution_context, task_input=input_record)

        # We need to provide a scope in which we guarantee the availability of resources,
        # such as temporary files provided for input, or other internally-generated
        # asyncio entities.
        async with input_resources as subprocess_input:
            logger.debug('Creating coroutine for {}'.format(task_type.__class__.__name__))
            # TODO: Use abstract task factory.
            coroutine = operations.subprocessCoroutine(context=execution_context, signature=subprocess_input)
            logger.debug('Creating asyncio Task for {}'.format(repr(coroutine)))
            awaitable = asyncio.create_task(coroutine)

            # TODO: Use abstract results handler.
            logger.debug('Waiting for task to complete.')
            result = await awaitable
            subprocess_exception = awaitable.exception()
            if subprocess_exception is not None:
                logger.exception('subprocess task raised exception {}'.format(str(subprocess_exception)))
                raise subprocess_exception
            logger.debug('Setting result for {}'.format(str(item)))
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
            logger.debug(f'Entering {task_dir}')
            # Note: This limits us to one task-per-process.
            # TODO: Use one (or more) subprocesses per task, setting the working directory.
            # The forking may be specialized to the task type, or we can default
            # to just always forking the Python interpreter.
            with scoped_chdir(task_dir):
                # TODO: Use implementation registry.
                # We should have already checked for this...
                namespace_end = len(task_type.as_tuple())
                implementation = None
                while namespace_end >= 1:
                    try:
                        module = '.'.join(task_type.as_tuple()[:namespace_end])
                        logger.debug(f'Looking for importable module: {module}')
                        implementation = importlib.import_module(module)
                    except ImportError:
                        implementation = None
                    else:
                        break
                    # Try the parent namespace?
                    namespace_end -= 1
                if namespace_end < 1:
                    raise InternalError('Bad task_type_identifier.')
                logger.debug(f'Looking for "scalems_helper in {module}.')
                helper = getattr(implementation, 'scalems_helper', None)
                if not callable(helper):
                    raise MissingImplementationError(
                        'Executor does not have an implementation for {}'.format(str(task_type)))
                else:
                    logger.debug('Helper found. Passing item to helper for execution.')
                    # Note: we need more of a hook for two-way interaction here.
                    try:
                        local_resources = execution_context.workflow_manager
                        result = helper(item, context=local_resources)
                        item.set_result(result)
                    except Exception as e:
                        raise e
                    execution_context.done(item.done())
        except Exception as e:
            logger.exception(f'Badly handled exception: {e}')
            raise e


@contextlib.contextmanager
def scoped_chdir(directory: typing.Union[str, bytes, os.PathLike]):
    """Restore original working directory when exiting the context manager.

    Not thread safe.

    Not compatible with concurrency patterns.
    """
    path = pathlib.Path(directory)
    if not path.exists() or not path.is_dir():
        raise ValueError(f'Not a valid directory: {directory}')
    original_dir = os.getcwd()
    try:
        os.chdir(path)
        yield path
    finally:
        os.chdir(original_dir)


def executor_factory(context: _context.WorkflowManager):
    executor = LocalExecutor(
        source_context=context,
        loop=context.loop(),
        dispatcher_lock=context._dispatcher_lock
    )
    return executor


class LocalExecutor:
    """Client side manager for work dispatched locally.
    """
    source_context: _context.WorkflowManager
    submitted_tasks: typing.List[asyncio.Task]

    _command_queue: asyncio.Queue
    _dispatcher_lock: asyncio.Lock
    _queue_runner_task: asyncio.Task

    def __init__(self,
                 source_context: _context.WorkflowManager,
                 loop: asyncio.AbstractEventLoop,
                 dispatcher_lock=None,
                 ):
        """Create a client side execution manager.

        Initialization and deinitialization occurs through
        the Python (async) context manager protocol.
        """
        self.source_context = source_context

        # TODO: Only hold a queue in an active context manager.
        self._command_queue = asyncio.Queue()

        self._loop: asyncio.AbstractEventLoop = loop

        self.session = None
        self._finalizer = None

        if not isinstance(dispatcher_lock, asyncio.Lock):
            raise TypeError('An asyncio.Lock is required to control dispatcher state.')
        self._dispatcher_lock = dispatcher_lock

        self._exception = None
        self.submitted_tasks = []

    def queue(self):
        # TODO: Only expose queue while in an active context manager.
        return self._command_queue

    async def __aenter__(self):
        try:
            # Get a lock while the state is changing.
            # The dispatching protocol is immature. Initially, we don't expect contention for the lock, and if there is
            # contention, it probably represents an unintended race condition or systematic dead-lock.
            # TODO: Clarify dispatcher state machine and remove/replace assertions.
            assert not self._dispatcher_lock.locked()
            async with self._dispatcher_lock:
                # self._connect_rp()
                # if self.session is None or self.session.closed:
                #     raise ProtocolError('Cannot process queue without a RP Session.')

                # Launch queue processor (proxy executor).
                runner_started = asyncio.Event()
                # task_manager = self.session.get_task_managers(tmgr_uids=self._task_manager_uid)
                runner_task = asyncio.create_task(run_executor(
                    executor=self,
                    processing_state=runner_started,
                    queue=self._command_queue
                ))
                await runner_started.wait()
                self._queue_runner_task = runner_task

            # Note: it would probably be most useful to return something with a WorkflowManager
            # interface...
            return self
        except Exception as e:
            self._exception = e
            raise e

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up at context exit.

        In addition to handling exceptions, the main resource to clean up is the rp.Session.

        We also need to make sure that we properly disengage from any queues or generators.

        We can also leave flags for ourself to be checked at __await__, if there is a Task
        associated with the Executor.
        """
        # Note that this coroutine could take a long time and could be cancelled at several points.
        cancelled_error = None
        # The dispatching protocol is immature. Initially, we don't expect contention for the lock, and if there is
        # contention, it probably represents an unintended race condition or systematic dead-lock.
        # TODO: Clarify dispatcher state machine and remove/replace assertions.
        assert not self._dispatcher_lock.locked()
        async with self._dispatcher_lock:

            # if session is None or session.closed:
            #     logger.error('rp.Session is already closed?!')
            # else:
            try:
                # Stop the executor.
                logger.debug('Stopping the SCALEMS local executor.')
                # TODO: Make sure that nothing else will be adding items to the queue from this point.
                # We should establish some assurance that the next line represents the last thing
                # that we will put in the queue.
                self._command_queue.put_nowait({'control': 'stop'})
                # TODO: Consider what to do differently when we want to cancel work rather than just finalize it.

                # done, pending = await asyncio.wait(aws, timeout=0.1, return_when=FIRST_EXCEPTION)
                # Currently, the queue runner does not place subtasks, so there is only one thing to await.
                # TODO: We should probably allow the user to provide some sort of timeout, or infer one from other time limits.
                try:
                    await self._queue_runner_task
                except asyncio.CancelledError as e:
                    raise e
                except Exception as queue_runner_exception:
                    logger.exception('Unhandled exception when stopping queue handler.', exc_info=True)
                    self._exception = queue_runner_exception
                else:
                    logger.debug('Queue runner task completed.')
                finally:
                    if not self._command_queue.empty():
                        logger.error('Command queue never emptied.')

                    # Wait for the watchers.
                    if len(self.submitted_tasks) > 0:
                        results = await asyncio.gather(*self.submitted_tasks)

            except asyncio.CancelledError as e:
                logger.debug(f'{self.__class__.__qualname__} context manager received cancellation while exiting.')
                cancelled_error = e
            except Exception as e:
                logger.exception('Exception while stopping dispatcher.', exc_info=True)
                if self._exception:
                    logger.error('Queuer is already holding an exception.')
                else:
                    self._exception = e
        if cancelled_error:
            raise cancelled_error

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False

    def active(self) -> bool:
        session = self.session
        if session is None:
            return False
        else:
            assert session is not None
            return not session.closed

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

    def exception(self) -> typing.Union[None, Exception]:
        return self._exception
