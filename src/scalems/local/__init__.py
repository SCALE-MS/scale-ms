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
import contextlib
import contextvars
import dataclasses
import importlib
import json
import logging
import queue
import threading
import warnings
import weakref
from typing import Any, Callable

import scalems.context
import typing
from scalems.core.exceptions import DuplicateKeyError, InternalError, MissingImplementationError, ProtocolError
from scalems.serialization import Encoder

from . import operations

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


class QueueItem(typing.Dict[str, Any]):
    """Queue items are either workflow items or control messages."""

    def _hexify(self):
        for key, value in self.items():
            if isinstance(value, bytes):
                value = value.hex()
            yield key, value

    def __str__(self) -> str:
        return str(dict(self._hexify()))


# TO DO NEXT: Implement the queue, dispatcher, executor (and internal queue)
# and, come on... add the fingerprinter... and the basic serializer...  `


class AsyncWorkflowManager(scalems.context.WorkflowManager):
    """Standard basic workflow context for local execution.

    Uses the asyncio module to allow commands to be staged as asyncio coroutines.

    There is no implicit OS level multithreading or multiprocessing.
    """
    def __init__(self):
        # Basic Context implementation details
        self.task_map = dict()  # Map UIDs to task Futures.
        # Note: We actually need multiple queues and a queue monitor to move
        # items between queues. The Executor will have a sense of "scope" for
        # tasks that are grouped by data locality or resource requirements, as
        # well as sub-graph scope. We also need queue(s) for responses from the
        # executor with which to update the local work graph state.
        # When the executor is launched, we can spool the current work graph into
        # a queue and install a hook for client-side work graph modifications to
        # also go into the queue. When the executor stops, we need to uninstall that
        # hook and drain the response queue. I think we need a second async task
        # to move items from a queue.SimpleQueue to a asyncio.Queue to support the hook.
        # It might be elegant to do this by calling the `add_item` of the ExecutionContext.
        # Instead of (at least part of) the return queue, we could proxy asyncio.Task.add_done_callback
        # with the returned ItemView.
        # TODO: Consider a more abstract event hook.
        self._queue: typing.Union[queue.Queue, None] = None
        # Consider just providing a queue manager and default dispatcher for use
        # by the base class.
        self._dispatcher: typing.Union[weakref.ref, None] = None
        self._dispatcher_lock = asyncio.Lock()
        # Rely on the GIL to provide a simple event hook.
        # self._event_hooks = {'add_item': {}}

    @contextlib.asynccontextmanager
    async def dispatch(self):
        """Start the executor task, then provide a scope for concurrent activity.

        Provide the executor with any currently-managed work in a queue.
        While the scope is active, new work added to the queue will be picked up
        by the executor.

        When leaving the `with` block, trigger the executor clean-up and wait for its task to complete.

        .. todo:: Clarify re-entrance policy, thread-safety, etcetera, and enforce.

        .. todo:: Allow an externally provided dispatcher factory, or even a running dispatcher?

        """
        # 1. Install a hook to catch new calls to add_item (the dispatcher_queue) and try not to yield until the current workflow state is obtained.
        # 2. Get snapshot of current workflow state with which to initialize the dispatcher. (It is now okay to yield.)
        # 3. Bind a new executor to its queue.
        # 4. Bind a dispatcher to the executor and the dispatcher_queue.
        # 5. Allow the executor and dispatcher to start using the event loop.

        # Avoid race conditions while checking for a running dispatcher.
        async with self._dispatcher_lock:
            # Dispatching state may be reentrant, but it does not make sense to re-enter through this call structure.
            if self._dispatcher is not None:
                raise ProtocolError('Already dispatching through {}.'.format(repr(self._dispatcher())))
            # For an externally-provided dispatcher:
            #     else:
            #         self._dispatcher = weakref.ref(dispatcher)

            # 1. Install a hook to catch new calls to add_item
            if self._queue is not None:
                raise ProtocolError('Found unexpected dispatcher queue.')
            dispatcher_queue = queue.SimpleQueue()
            self._queue = dispatcher_queue

            # 2. Get snapshot of current workflow state with which to initialize the dispatcher.
            # TODO: Topologically sort DAG!
            initial_task_list = list(self.task_map.keys())
            #  It is now okay to yield.

            # 3. Bind a new executor to its queue.
            # Note: if there were a reason to decouple the executor lifetime from this scope,
            # we could consider a more object-oriented interface with it.
            executor_queue = asyncio.Queue()
            for _task_id in initial_task_list:
                await executor_queue.put(QueueItem({'add_item': _task_id}))
            executor = run_executor(source_context=self, command_queue=executor_queue)

            # 4. Bind a dispatcher to the executor_queue and the dispatcher_queue.
            # TODO: We should bind the dispatcher directly to the executor, but that requires
            #  that we make an Executor class with concurrency-safe methods.
            # dispatcher = run_dispatcher(dispatcher_queue, executor_queue)
            # self._dispatcher = weakref.ref(dispatcher)
            # TODO: Toggle active dispatcher state.
            # scalems.context._dispatcher.set(...)

            # 5. Allow the executor and dispatcher to start using the event loop.
            executor_task = asyncio.create_task(executor)
            # asyncio.create_task(dispatcher)

        try:
            # We can surrender control here and leave the executor and dispatcher tasks running
            # while evaluating a `with` block suite for the `dispatch` context manager.
            yield

        except Exception as e:
            logger.exception('Uncaught exception while in dispatching context: {}'.format(str(e)))
            raise e

        finally:
            async with self._dispatcher_lock:
                self._dispatcher = None
                self._queue = None
            # dispatcher_queue.put({'control': 'stop'})
            # await dispatcher
            # TODO: Make sure the dispatcher hasn't died. Look for acknowledgement
            #  of receipt of the Stop command.
            # TODO: Check status...
            if not dispatcher_queue.empty():
                logger.error('Dispatcher finished while items remain in dispatcher queue. Approximate size: {}'.format(dispatcher_queue.qsize()))

            # Stop the executor.
            executor_queue.put_nowait({'control': 'stop'})
            await executor_task
            if executor_task.exception() is not None:
                raise executor_task.exception()

            # Check that the queue drained.
            # WARNING: The queue will never finish draining if executor_task fails.
            #  I.e. don't `await executor_queue.join()`
            if not executor_queue.empty():
                raise InternalError('Bug: Executor left tasks in the queue without raising an exception.')

            logger.debug('Exiting {} dispatch context.'.format(type(self).__name__))

            # if loop.is_running():
            #     # Clean up unawaited tasks.
            #     loop.run_until_complete(loop.shutdown_asyncgens())
            #     # Do we need to check the work graph directly?
            #     # We need to make sure the loop is not running before calling close()
            #     loop.stop()
            # Do we want to close the event loop here, as part of scalems.run(), or somewhere else?
            # loop.close()

    def item(self, uid: bytes):
        """Interact with a managed item.

        In the initial implementation, this supports both client ItemViews and
        executor updates, which may not be appropriate.
        """
        # Consider providing the consumer context when acquiring access.
        # Consider limiting the scope of access requested.
        return self.task_map[uid]

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


# TODO: Consider explicitly decoupling client-facing workflow manager from executor-facing manager.
async def run_executor(source_context: AsyncWorkflowManager, command_queue: asyncio.Queue):
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
    # for queues that potentially never yield any items.
    while True:
        command: QueueItem = await command_queue.get()
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
            item = source_context.item(key)
            if not isinstance(item, scalems.context.Task):
                raise InternalError('Expected {}.item() to return a scalems.context.Task'.format(repr(source_context)))

            # TODO: Ensemble handling
            item_shape = item.description().shape()
            if len(item_shape) != 1 or item_shape[0] != 1:
                raise MissingImplementationError('Executor cannot handle multidimensional tasks yet.')

            task_type_identifier = item.description().type().identifier()
            # Note that we could insert resource management here. We could create
            # tasks until we run out of free resources, then switch modes to awaiting
            # tasks until resources become available, then switch back to placing tasks.
            await _execute_item(task_type_identifier=task_type_identifier,
                                item=item,
                                execution_context=source_context)

        finally:
            logger.debug('Releasing "{}" from command queue.'.format(str(command)))
            command_queue.task_done()


async def _execute_item(task_type_identifier, item, execution_context):
    # TODO: Automatically resolve resource types.
    if task_type_identifier == 'scalems.subprocess.SubprocessTask':
        task_type = scalems.subprocess.SubprocessTask()

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
            coroutine = operations.subprocessCoroutine(subprocess_input)
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
    else:
        try:
            implementation = importlib.import_module(str(task_type_identifier))
        except ImportError:
            # We should have already checked for this...
            implementation = None
        helper = getattr(implementation, 'scalems_helper', None)
        if not callable(helper):
            raise MissingImplementationError(
                'Executor does not have an implementation for {}'.format(str(task_type_identifier)))
        else:
            helper(item)
