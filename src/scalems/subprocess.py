"""Define the ScaleMS Subprocess command.

scalems.subprocess() is used to execute a program in one (or more) subprocesses.
It is an alternative to the built-in Python subprocess.Popen or asyncio.create_subprocess_exec
with extensions to better support ScaleMS execution dispatching and ensemble data flow.

In the first iteration, we can use dataclasses.dataclass to define input/output data structures
in terms of standard types. In a follow-up, we can use a scalems metaclass to define them
in terms of Data Descriptors that support mixed scalems.Future and native constant data types.
"""
import logging
import typing
from dataclasses import dataclass, field
from pathlib import Path # We probably need a scalems abstraction for Path.

from .exceptions import MissingImplementationError
from .context import get_context

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


# TODO: what is the mechanism for registering a command implementation in a new Context?
# TODO: What is the relationship between the command factory and the command type? Which parts need to be importable?

@dataclass
class SubprocessInput:
    # TODO: Move input documentation to Input class docs.
    argv: typing.Sequence[str]
    inputs: typing.Mapping[str, Path] = field(default_factory=dict)
    outputs: typing.Mapping[str, Path] = field(default_factory=dict)
    stdin: typing.Iterable[str] = ()
    environment: typing.Mapping[str, typing.Union[str, None]] = field(default_factory=dict)
    # For now, let's just always enable stdout/stderr
    # stdout: Optional[Path]
    # stderr: Optional[Path]
    resources: typing.Mapping[str, typing.Any] = field(default_factory=dict)


@dataclass
class SubprocessResult:
    # file: Field(Path)
    # exitcode: Field(int)
    # TODO: Can we use None instead of os.devnull to indicate non-presence of stdout/stderr?
    exitcode: int
    stdout: Path
    stderr: Path
    file: typing.Mapping[str, Path]


class SubprocessResourceType:
    """Describe the type of resource provided by a Subprocess command."""
    @classmethod
    def as_strings(cls):
        return ('scalems', 'subprocess')

    @classmethod
    def identifier(cls):
        return '.'.join(cls.as_strings())


class Subprocess:
    @classmethod
    def type(self):
        return SubprocessResourceType

    @classmethod
    def input_type(cls):
        return SubprocessInput

    @classmethod
    def result_type(cls):
        return SubprocessResult

    def __init__(self, input: SubprocessInput):
        self._bound_input = input
        self._result = None

    def input_collection(self):
        return self._bound_input

    def result(self):
        return self._result

    def dependencies(self):
        ...

    def uid(self):
        return '0'*64

    def serialize(self) -> str:
        """Encode the task as a JSON record.

        Input and Result will be serialized as references.
        The caller is responsible for serializing existing records
        for bound objects, if they exist.
        """
        record = {}
        record['uid'] = self.uid()
        # "label" not yet supported.
        record['input'] = self._bound_input # reference
        record['result'] = self._result # reference
        raise MissingImplementationError('To do...')

    @classmethod
    def deserialize(cls, record: str, context = None):
        """Instantiate a Subprocess Task from a serialized record.

        In general, records should only be deserialized into a WorkflowContext
        that manages a valid work graph, but for early testing, at least,
        we have some standalone use cases.
        """

        # The record may or may not have a bound result.
        # If there is a bound result, it should be added to the workgraph first.
        return cls()

    # def __await__(self) -> typing.Generator[typing.Any, None, SubprocessResult]:
    #     """Implements the asyncio protocol for a coroutine object.
    #
    #     When awaited, query the current context to negotiate dispatching. Note that the
    #     built-in asyncio module acts like a LocalExecutor Context if and only if there
    #     is not an active SCALE-MS Context. SCALE-MS Contexts
    #     set the current context before awaiting.
    #     """
    #     # TODO: implementation registration.
    #     import scalems.context
    #     import scalems.local
    #     import scalems.radical
    #     context = scalems.context.get_context()
    #     # TODO: dispatching
    #     if isinstance(context, scalems.local.LocalExecutor):
    #         from scalems.local.operations import executable as local_exec
    #         # Note that we need a more sophisticated coroutine object than what we get directly from `async def`
    #         # for command instances that can present output in multiple contexts or be transferred from one to another.
    #         self._result = local_exec(self)
    #     elif isinstance(context, scalems.radical.RPExecutor):
    #         from scalems.radical.operations import executable as _rp_exec
    #         self._result = _rp_exec(self)
    #     else:
    #         raise MissingImplementationError('Current context {} does not implement scalems.executable'.format(context))
    #
    #     # Allow this function to be a generator function, fulfilling the awaitable protocol.
    #     yield self
    #     # Note that the items yielded are not particularly useful, but the position of the
    #     # yield expression(s) is potentially useful for debugging or multitasking. Depending
    #     # on the implementation of the event loop, multiple yields may allow a way to avoid
    #     # deadlocks. For instance, we may choose to yield at each iteration of a loop to
    #     # provide or read PIPE-based stdin or stdout. `await` should accomplish the same thing,
    #     # but the generator protocol may improve debugging and generality.
    #     # The point of "yield" is more interesting when we use "yield" as an expression in the
    #     # yielding code, which allows values to be passed in to the coroutine at the evaluation
    #     # of the yield expression (e.g. https://docs.python.org/3/howto/functional.html#passing-values-into-a-generator
    #     # but not that the coroutine protocol is slightly different, per https://www.python.org/dev/peps/pep-0492/)
    #     # For instance, this could be a mechanism for nesting event loops or dispatching contexts
    #     # while maintaining a heart-beat or other command-channel-like wrapper.
    #
    #     if not isinstance(self._result, SubprocessResult):
    #         raise RuntimeError('Result was not delivered!')
    #     return self._result



def executable(*args, context=None, **kwargs):
    """Execute a command line program.

    Note:
        Most users will prefer to use the commandline_operation() helper instead
        of this low-level function.

    Configure an executable to run in one (or more) subprocess(es).
    Executes when run in an execution Context, as part of a work graph.
    Process environment and execution mechanism depends on the execution environment,
    but is likely similar to (or implemented in terms of) the POSIX execvp system call.

    Shell processing of *argv* is disabled to improve determinism.
    This means that shell expansions such as environment variables, globbing (``*``),
    and other special symbols (like ``~`` for home directory) are not available.
    This allows a simpler and more robust implementation, as well as a better
    ability to uniquely identify the effects of a command line operation. If you
    think this disallows important use cases, please let us know.

    Required Arguments:
         argv: a tuple (or list) to be the subprocess arguments, including the executable

    Optional Arguments:
         outputs: labeled output files, mapping command line flag to one (or more) filenames
         inputs: labeled input files, mapping command line flag to one (or more) filenames
         environment: environment variables to be set in the process environment
         stdin: source for posix style standard input file handle (default None)
         stdout: Capture standard out to a filesystem artifact, even if it is not consumed in the workflow.
         stderr: Capture standard error to a filesystem artifact, even if it is not consumed in the workflow.
         resources: Name additional required resources, such as an MPI environment.

    .. todo:: Support POSIX sigaction / IPC traps?

    .. todo:: Consider dataclasses.dataclass types to replace reusable/composable function signatures.

    Program arguments are iteratively added to the command line with standard Python
    iteration, so you should use a tuple or list even if you have only one parameter.
    I.e. If you provide a string with ``arguments="asdf"`` then it will be passed as
    ``... "a" "s" "d" "f"``. To pass a single string argument, ``arguments=("asdf")``
    or ``arguments=["asdf"]``.

    *inputs* and *outputs* should be a dictionary with string keys, where the keys
    name command line "flags" or options.

    Note that the Execution Context (e.g. RPContext, LocalContext, DockerContext)
    determines the handling of *resources*. Typical values in *resources* may include
    * procs_per_task (int): Number of processes to spawn for an instance of the *exec*.
    * threads_per_proc (int): Number of threads to allocate for each process.
    * gpus_per_task (int): Number of GPU devices to allocate for and instance of the *exec*.
    * launcher (str): Task launch mechanism, such as `mpiexec`.

    Returns:
        Output collection contains *exitcode*, *stdout*, *stderr*, *file*.

    The *file* output has the same keys as the *outputs* key word argument.

    Example:
        Execute a command named ``exe`` that takes a flagged option for input
        and output file names
        (stored in a local Python variable ``my_filename`` and as the string literal ``'exe.out'``)
        and an ``origin`` flag
        that uses the next three arguments to define a vector.

            >>> my_filename = "somefilename"
            >>> command = scalems.executable(('exe', '--origin', 1.0, 2.0, 3.0),
            ...                              inputs={'--infile': scalems.file(my_filename)},
            ...                              outputs={'--outfile': scalems.file('exe.out')})
            >>> assert hasattr(command, 'file')
            >>> import os
            >>> assert os.path.exists(command.file['--outfile'].result())
            >>> assert hasattr(command, 'exitcode')

    TODO:
        Consider input/output files that do not appear on the command line, but which must figure into data flow.

    """
    # TODO: Dispatch correctly if a SubprocessInput is provided.
    # TODO: Use Python overload type hints.
    bound_input = SubprocessInput(*args, **kwargs)

    # TODO: Implement TaskBuilder director.
    if context is None:
        context = get_context()
    # TODO: The returned value should be a TaskView provided by the Context with
    #       minimal state or ownership semantics.
    task = context.add_task(Subprocess(bound_input))

    return task
