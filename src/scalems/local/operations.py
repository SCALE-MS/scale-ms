"""
Specialize implementations of ScaleMS operations.

"""
import scalems.subprocess
from scalems.exceptions import DispatchError


def local_exec(task_description: dict):
    argv = task_description['args']
    assert isinstance(argv, (list, tuple))
    assert len(argv) > 0
    import subprocess
    # TODO: Consider whether we want to support buffered I/O streams (pipes).
    return subprocess.run(argv)


def make_subprocess_args(context, task_input: scalems.subprocess.SubprocessInput):
    """InputResource factory for *subprocess* based implementations.
    """
    # subprocess.Popen and asyncio.create_subprocess_exec have approximately compatible arguments.
    # from scalems.context.local import AbstractLocalContext
    # if not isinstance(context, AbstractLocalContext):
    #     raise ValueError('This resource factory is for subprocess-based execution per scalems.context.local')
    # TODO: await the arguments.
    args = list([arg for arg in task_input.argv])

    # TODO: stream based input with PIPE.
    kwargs = {
        'stdin': None,
        'stdout': None,
        'stderr': None,
        'env': None
    }
    return {'args': args, 'kwargs': kwargs}


async def get_coroutine(task_description: dict):
    """Create and execute a subprocess task in the context."""
    import asyncio
    argv = task_description['args']
    assert isinstance(argv, (list, tuple))
    assert len(argv) > 0
    # Get callable.
    process = await asyncio.create_subprocess_exec(*argv)
    returncode = await process.wait()
    result = scalems.subprocess.SubprocessResult(exitcode=returncode, stdout=None, stderr=None, file={})
    # TODO: We should yield in here, somehow, to allow cancellation of the subprocess.
    # Suggest splitting runner into separate launch/resolve phases or representing this
    # long-running function as a stateful object. Note: this is properly a run-time Task.
    # TODO: Split current Context implementations into two components: run time and dispatcher?
    # Or is that just the Context / Session division?
    # Run time needs to allow for task management (asynchronous where applicable) and can
    # be confined to an environment with a running event loop (where applicable) and/or
    # active executor.
    # Dispatcher would be able to insulate callers / collaborators from event loop details.
    assert result.exitcode is not None
    return result


def executable(context, task: scalems.subprocess.Subprocess):
    # Make inputs.
    # Translate SubprocessInput to the Python subprocess function signature.
    subprocess_input = make_subprocess_args(context=context, task_input=task.input_collection())
    # Run subprocess.
    if isinstance(context, scalems.local.ImmediateExecutionContext):
        handle = local_exec(subprocess_input)
    elif isinstance(context, scalems.local.AsyncWorkflowContext):
        handle = get_coroutine(subprocess_input)
    else:
        raise DispatchError('Cannot dispatch for context {}'.format(repr(context)))
    # Return SubprocessResult object.
    return handle
