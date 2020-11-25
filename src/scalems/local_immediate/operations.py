"""
Specialize implementations of ScaleMS operations.

"""
import scalems.subprocess
from scalems.core.exceptions import DispatchError


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


def executable(context, task: scalems.subprocess.Subprocess):
    # Make inputs.
    # Translate SubprocessInput to the Python subprocess function signature.
    subprocess_input = make_subprocess_args(context=context, task_input=task.input_collection())
    # Run subprocess.
    if isinstance(context, scalems.local_immediate.ImmediateExecutionContext):
        handle = local_exec(subprocess_input)
    else:
        raise DispatchError('Cannot dispatch for context {}'.format(repr(context)))
    # Return SubprocessResult object.
    return handle
