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
import typing

import scalems.subprocess
import scalems.workflow
from scalems.exceptions import InternalError
from scalems.exceptions import ProtocolError
from scalems.subprocess._subprocess import OutputFile
from scalems.unique import next_monotonic_integer as _next_int


@dataclasses.dataclass
class SubprocessInput:
    """Implementation-specific form of input for scalems.subprocess.Subprocess."""

    program: pathlib.Path
    args: typing.Sequence[str]
    stdin: typing.Union[None, typing.TextIO]
    stdout: typing.Union[None, typing.TextIO]
    stderr: typing.Union[None, typing.TextIO]
    env: typing.Union[None, typing.Mapping[str, str]]


class _ExecutionContext:
    """Represent the run time environment for a managed workflow item.

    Provide local definition of evolving concept from local/__init__.py
    """

    workflow_manager: scalems.workflow.WorkflowManager
    identifier: bytes


async def subprocessCoroutine(context: _ExecutionContext, signature: SubprocessInput):
    """Implement Subprocess in the local execution context.

    Use this coroutine as the basis for a Task that will provide the Future[SubprocessResult] interface.
    """
    task_directory = os.path.abspath(context.identifier.hex())

    kwargs = {"stdin": signature.stdin, "stdout": signature.stdout, "stderr": signature.stderr, "env": signature.env}
    if signature.env is not None:
        kwargs["env"] = {key: value for key, value in signature.env.items()}

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
        env=signature.env,
        cwd=task_directory,
    )

    try:
        returncode = await process.wait()
        # TODO: Error handling.
        # For now, assume non-zero exit codes are equivalent to task failure.
    except Exception as e:
        # TODO: Repackage as a plausibly temporary failure?
        raise e
    else:
        # Alternative: Mark task as "done" if no exception was raised.
        if returncode == 0:
            # TODO: How to do the output file staging?
            stdout = pathlib.Path(os.path.abspath(signature.stdout.name))
            stderr = pathlib.Path(os.path.abspath(signature.stderr.name))
            # What do we return for a failed task? Maybe raise?
            result = scalems.subprocess.SubprocessResult(exitcode=returncode, stdout=stdout, stderr=stderr, file={})
            return result
        else:
            raise InternalError("No handler for failed tasks.")


@contextlib.asynccontextmanager
async def input_resource_scope(  # noqa: C901
    context: _ExecutionContext,
    task_input: typing.Union[scalems.subprocess.SubprocessInput, typing.Awaitable[scalems.subprocess.SubprocessInput]],
):
    """Manage the actual execution context of the asyncio.subprocess.Process.

    Translate a scalems.subprocess.SubprocessInput to a local SubprocessInput instance.

    InputResource factory for *subprocess* based implementations.

    TODO: How should this be composed in terms of the context and (local) resource type?
        Note that the (currently unused) *context* parameter is available for dispatching.
    """
    # Await the inputs.
    if inspect.isawaitable(task_input):
        task_input = await task_input
    # TODO: Use an awaitable to deliver the SubprocessInput providing the arguments.
    # Consider providing generic awaitable behavior through a decorator or base class
    # so we don't need to do extra checks and can just await all scalems objects.
    # TODO: What sort of validation or normalization do we want to do for the executable name?
    if not isinstance(task_input, scalems.subprocess.SubprocessInput):
        raise InternalError("Unexpected input type.")
    path = shutil.which(task_input.argv[0])
    if path is None or not os.path.exists(path):
        raise InternalError("Could not find executable. Input should be vetted before this point.")
    program = pathlib.Path(shutil.which(task_input.argv[0]))
    args = list(str(arg) for arg in task_input.argv[1:])

    task_directory = os.path.abspath(context.identifier.hex())
    if os.path.exists(task_directory):
        # check if done
        done_file = os.path.join(task_directory, "done")
        if os.path.exists(done_file):
            raise InternalError("Refusing to prepare input resource scope for a completed task.")
        # check if clean
        # currently deferred to specific files below.
        ...
    else:
        # make task directory
        os.mkdir(task_directory)

    # TODO: Make sure we have a good way to handle inputs from other tasks.
    if task_input.inputs is not None and len(task_input.inputs) > 0:
        for key, value in task_input.inputs.items():
            try:
                if not os.path.exists(value):
                    raise ValueError("File not found: {}".format(value))
            except Exception as e:
                raise TypeError("Invalid input (expected file path): {}".format(repr(value))) from e
            args.extend([key, value])

    if task_input.outputs is not None and len(task_input.inputs) > 0:
        for i, (key, value) in enumerate(task_input.outputs.items()):
            if isinstance(value, OutputFile):
                if value.label is not None:
                    # TODO: Add an item to the graph that depends on the task.
                    ...
                # Generate an appropriate output file name.
                # Use the fingerprint of the task_input, the current operation type identifier,
                # and the sequence number of this output file in the current command.

                # Note that this means we need an additional interaction with the workflow manager
                # in order to have enough context to associate the output with the operation that is
                # consuming these input resources. Alternatively, we could broaden the scope of this
                # context manager to represent all run-time resources, and not just the input resources.

                # input_hash = hash(task_input)
                # input_hash += hash('Subprocess')
                # input_hash += hash(i)
                # basename = hashlib.sha256(bytes(input_hash)).digest().hex()
                # TODO: Handle working directory.
                filename = _next_int().to_bytes(32, "big").hex() + value.suffix

                path = os.path.join(task_directory, filename)
                if os.path.exists(path):
                    raise ProtocolError("Output file already exists but reexecution has not been considered.")
                # There is obviously a race condition / security hole between the time
                # that we check an output file name and execute the command. This is
                # unavoidable with process that expects to create a file with a user-provided name.
            else:
                try:
                    path = pathlib.Path(str(value))
                    if not path.is_absolute():
                        path = os.path.join(task_directory, path)
                    if os.path.exists(path):
                        raise ValueError("Output file already exists: {}".format(path))
                except Exception as e:
                    raise TypeError("Invalid input (expected file path): {}".format(repr(value))) from e
            args.extend([key, path])

    # Warning: If subprocess.Popen receives *env* argument that is not None, it **replaces** the
    # default environment (a duplicate of the caller's environment). We might want to provide
    # fancier semantics to copy or reject select variables from the environment or to
    # dynamically read the default environment at execution time (note that the results of
    # such an operation would not represent a unique result!)
    @contextlib.contextmanager
    def get_env() -> dict:
        # TODO: Use provided environment details.
        yield None

    # Get stdin context manager.
    if task_input.stdin is None:

        @contextlib.contextmanager
        def get_stdin():
            yield None

    else:
        # TODO: Strengthen the typing for stdin parameter.
        if not isinstance(task_input.stdin, (os.PathLike, str, list, tuple)):
            # Note: this is an internal error indicating a bug in SCALEMS.
            raise InternalError("No handler for stdin argument of this form." + repr(task_input.stdin))
        if isinstance(task_input.stdin, (list, tuple)):
            # TODO: Assign appropriate responsibility for maintaining filesystem artifacts.
            infile = os.path.join(task_directory, "stdin")
            with open(infile, "w") as fp:
                # Normalize line endings for local environment.
                fp.writelines([line.rstrip() + "\n" for line in task_input.stdin])
        else:
            infile = os.path.abspath(task_input.stdin)

        def get_stdin():
            return open(os.path.abspath(infile), "r")

    def get_output_file_contextmanager(filename):
        if not isinstance(filename, (os.PathLike, str)):
            # Note: this is an internal error indicating a bug in SCALEMS.
            raise InternalError("No handler for path of this form. " + repr(filename))
        outfile = pathlib.Path(filename)
        if not outfile.is_absolute():
            outfile = os.path.join(task_directory, outfile)
        if os.path.exists(outfile):
            raise InternalError("Dirty working directory is not recoverable in current implementation.")

        def get_openfile():
            return open(outfile, "w")

        return get_openfile

    # Get stdout context manager.
    get_stdout = get_output_file_contextmanager(task_input.stdout or "stdout")

    # Get stderr context manager.
    get_stderr = get_output_file_contextmanager(task_input.stderr or "stderr")

    # Create scoped resources. This depends somewhat on the input.
    # For each non-None stdio stream, we need to provide an open file-like handle.
    # Note: there may be a use case for scoped run-time determination of *env*,
    # but we have not yet allowed for that.
    with get_stdin() as fh_in, get_stdout() as fh_out, get_stderr() as fh_err, get_env() as env:
        try:
            # TODO: Create SubprocessInput with a coroutine so that we can yield an awaitable.
            subprocess_input = SubprocessInput(program, args, stdin=fh_in, stdout=fh_out, stderr=fh_err, env=env)
            # Provide resources to the implementation coroutine.
            yield subprocess_input
        finally:
            # Clean up scoped resources.
            # Deliver output files?
            ...
