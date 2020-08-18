"""
Specialize implementations of ScaleMS operations.

"""
import scalems.subprocess


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


# def local_runner(session, arguments):
#     """Create and execute a subprocess task in the context."""
#     # Get callable.
#     process = session.wait(session.subprocess_runner(*arguments['args'], **arguments['kwargs']))
#     runner = process.wait
#     # Call callable.
#     # TODO: We should yield in here, somehow, to allow cancellation of the subprocess.
#     # Suggest splitting runner into separate launch/resolve phases or representing this
#     # long-running function as a stateful object. Note: this is properly a run-time Task.
#     # TODO: Split current Context implementations into two components: run time and dispatcher?
#     # Or is that just the Context / Session division?
#     # Run time needs to allow for task management (asynchronous where applicable) and can
#     # be confined to an environment with a running event loop (where applicable) and/or
#     # active executor.
#     # Dispatcher would be able to insulate callers / collaborators from event loop details.
#     session.wait(runner())
#
#     result = SubprocessResult()
#     result._exitcode = process.returncode
#     assert result.exitcode() is not None
#     return result


def executable(task: scalems.subprocess.Subprocess):
    # Make inputs.
    # Translate SubprocessInput to the Python subprocess function signature.
    subprocess_input = make_subprocess_args(context=None, task_input=task.input_collection())
    # Run subprocess.
    # Return SubprocessResult object.
    return local_exec(subprocess_input)