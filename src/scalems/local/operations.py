"""
Specialize implementations of ScaleMS operations.

"""
import asyncio
import contextlib
import dataclasses
import inspect
import os
import pathlib
import shutil
import tempfile
import typing

import scalems.subprocess
from scalems.exceptions import DispatchError, InternalError


@dataclasses.dataclass
class SubprocessInput:
    """Implementation-specific form of input for scalems.subprocess.Subprocess."""
    program: pathlib.Path
    args: typing.Sequence[str]
    stdin: typing.Union[None, typing.TextIO]
    stdout: typing.Union[None, typing.TextIO]
    stderr: typing.Union[None, typing.TextIO]
    env: typing.Union[None, typing.Mapping[str, str]]


async def subprocessCoroutine(signature: SubprocessInput):
    """Implement Subprocess in the local execution context.

    Use this coroutine as the basis for a Task that will provide the Future[SubprocessResult] interface.
    """
    kwargs = {

        'stdin': signature.stdin,
        'stdout': signature.stdout,
        'stderr': signature.stderr,
        'env': None
    }
    if signature.env is not None:
        kwargs['env'] = {key: value for key, value in signature.env.items()}

    # TODO: Institute some checks that the implementation is not using more resources
    #  than it was allocated.
    # TODO: Institute checks that the implementation is not generating workflow dependencies
    #  for itself that cannot be scheduled. (Consider a primitive allowing a task to surrender
    #  its resources and suspend itself, to be reconsidered for scheduling eligibility after
    #  its new dependencies have been evaluated.)
    process = await asyncio.create_subprocess_exec(
        os.fsencode(signature.program),
        *signature.args,
        stdin=signature.stdin,
        stdout=signature.stdout,
        stderr=signature.stderr,
        env=signature.env
    )
    returncode = await process.wait()

    # TODO: How to do the output file staging?
    stdout = pathlib.Path(os.path.abspath(signature.stdout.name))
    stderr = pathlib.Path(os.path.abspath(signature.stderr.name))
    result = scalems.subprocess.SubprocessResult(exitcode=returncode, stdout=stdout, stderr=stderr, file={})
    return result


@contextlib.asynccontextmanager
async def input_resource_scope(context, task_input: typing.Union[scalems.subprocess.SubprocessInput, typing.Awaitable[scalems.subprocess.SubprocessInput]]):
    """Manage the actual execution context of the asyncio.subprocess.Process.

    Translate a scalems.subprocess.SubprocessInput to a local SubprocessInput instance.

    InputResource factory for *subprocess* based implementations.

    TODO: How should this be composed in terms of the context and (local) resource type?
    """
    # Await the inputs.
    if inspect.isawaitable(task_input):
        task_input = await task_input
    # TODO: Use an awaitable to deliver the SubprocessInput providing the arguments.
    # Consider providing generic awaitable behavior through a decorator or base class
    # so we don't need to do extra checks and can just await all scalems objects.
    # TODO: What sort of validation or normalization do we want to do for the executable name?
    if not isinstance(task_input, scalems.subprocess.SubprocessInput):
        raise InternalError('Unexpected input type.')
    program = pathlib.Path(shutil.which(task_input.argv[0]))
    if not program.exists():
        raise InternalError('Could not find executable. Input should be vetted before this point.')
    args = list(str(arg) for arg in task_input.argv[1:])

    # Warning: If subprocess.Popen receives *env* argument that is not None, it **replaces** the
    # default environment (a duplicate of the caller's environment). We might want to provide
    # fancier semantics to copy or reject select variables from the environment or to
    # dynamically read the default environment at execution time (note that the results of
    # such an operation would not represent a unique result!)
    get_env = lambda : None

    get_stdin = lambda : None
    if task_input.stdin is not None:
        # TODO: Strengthen the typing for stdin parameter.
        if not isinstance(task_input.stdin, (os.PathLike, str, list, tuple)):
            # Note: this is an internal error indicating a bug in SCALEMS.
            raise InternalError('No handler for stdin argument of this form.' + repr(task_input.stdin))
        if isinstance(task_input.stdin, (list, tuple)):
            # TODO: Assign appropriate responsibility for maintaining filesystem artifacts.
            infile = 'stdin'
            with open(infile, 'w') as fp:
                # Normalize line endings for local environment.
                fp.writelines([line.rstrip() for line in task_input.stdin])
        else:
            infile = task_input.stdin
        get_stdin = lambda path=os.fsencode(infile): open(os.path.abspath(path), 'r')

    stdout = task_input.stdout
    if stdout is None:
        stdout = 'stdout'
    if not isinstance(task_input.stdout, (os.PathLike, str)):
        # Note: this is an internal error indicating a bug in SCALEMS.
        raise InternalError('No handler for stdout argument of this form. ' + repr(task_input.stdout))
    if os.path.exists(stdout):
        raise InternalError('Dirty working directory is not recoverable in current implementation.')
    get_stdout = lambda path=os.path.abspath(stdout) : open(path, 'w')

    stderr = task_input.stderr
    if stderr is None:
        stderr = 'stderr'
    if not isinstance(task_input.stderr, (os.PathLike, str)):
        # Note: this is an internal error indicating a bug in SCALEMS.
        raise InternalError('No handler for stderr argument of this form.' + repr(task_input.stderr))
    if os.path.exists(stderr):
        raise InternalError('Dirty working directory is not recoverable in current implementation.')
    get_stderr = lambda path=os.path.abspath(stderr) : open(path, 'w')

    # Create scoped resources. This depends somewhat on the input.
    # For each non-None stdio stream, we need to provide an open file-like handle.
    # Note: there may be a use case for scoped run-time determination of *env*,
    # but we have not yet allowed for that.
    with get_stdin() as fh_in, get_stdout() as fh_out, get_stderr() as fh_err:
        try:
            # TODO: Create SubprocessInput with a coroutine so that we can yield an awaitable.
            subprocess_input = SubprocessInput(program, args, stdin=fh_in, stdout=fh_out, stderr=fh_err, env=get_env())
            # Provide resources to the implementation coroutine.
            yield subprocess_input  # needs to be an awaitable... ?
        finally:
            # Clean up scoped resources.
            # Deliver output files?
            ...
