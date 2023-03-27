"""Define the ScaleMS Subprocess command.

`scalems.executable` is used to execute a program in one (or more) subprocesses.
It is an alternative to the built-in Python `subprocess.Popen` or `asyncio.create_subprocess_exec`
with extensions to better support ScaleMS execution dispatching and ensemble data flow.

The core task is represented by `scalems.subprocess.SubprocessTask`, but also
requires the definition of `scalems.subprocess.SubprocessInput` and
`scalems.subprocess.SubprocessResult`.



In the first iteration, we can use dataclasses.dataclass to define input/output data structures
in terms of standard types. In a follow-up, we can use a scalems metaclass to define them
in terms of Data Descriptors that support mixed scalems.Future and native constant data types.
"""

__all__ = ["executable", "Subprocess", "SubprocessInput", "SubprocessResult"]

import dataclasses
import json
import logging
import typing
from pathlib import Path  # We probably need a scalems abstraction for Path.

import scalems.workflow
from scalems.exceptions import InternalError
from scalems.serialization import encode
from ..unique import next_monotonic_integer
from ..exceptions import APIError
from ..exceptions import MissingImplementationError
from ..workflow import WorkflowManager

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


class OutputFile(dict):
    """Placeholder for output files.

    The initial implementation of OutputFile does not provide access to the
    created output file.

    The actual filename is managed by SCALE-MS to avoid namespace collisions.

    Arguments:
        label (str): Optional user-friendly identifier for locating a reference in the managed workflow.
        suffix (str): Optional filename suffix for the generated filename.

    In a future implementation, we may allow instances of OutputFile to transform
    into workflow references that are dependent on the task under construction.
    """

    def __init__(self, label=None, suffix=""):
        super().__init__()
        self["label"] = label
        self["suffix"] = suffix

    @property
    def label(self):
        return self.get("label", None)

    @property
    def suffix(self):
        return self.get("suffix", "")


# TODO: what is the mechanism for registering a command implementation in a new Context?
# TODO: What is the relationship between the command factory and the command type? Which parts need to be importable?


# TODO: input data typing.
@dataclasses.dataclass(frozen=True)
class SubprocessInput:
    # TODO: Move input documentation to Input class docs.
    argv: typing.Sequence[str]
    inputs: typing.Mapping[str, Path] = dataclasses.field(default_factory=dict)
    outputs: typing.Mapping[str, OutputFile] = dataclasses.field(default_factory=dict)
    stdin: typing.Iterable[str] = ()
    environment: typing.Mapping[str, typing.Union[str, None]] = dataclasses.field(default_factory=dict)
    # For now, let's just always enable stdout/stderr
    stdout: Path = dataclasses.field(default=Path("stdout"))
    stderr: Path = dataclasses.field(default=Path("stderr"))
    resources: typing.Mapping[str, typing.Any] = dataclasses.field(default_factory=dict)


# Register a director for SubprocessInput workflow items.
# TODO: Link to deserializer behavior.
# TODO: Normalize task_builder protocol.
# TODO: Generate from class decorator.
# TODO: Need a generic TaskView class for clients with reference to managed elements.
@scalems.workflow.workflow_item_director_factory.register
def _(item: SubprocessInput, *, manager: WorkflowManager, label: str = None):
    assert isinstance(manager, WorkflowManager)

    def director(*args, **kwargs):
        if len(args) > 0:
            # TODO: Reconsider reasonable exceptions.
            raise TypeError("Unexpected positional arguments.")
        if len(kwargs) > 0:
            raise TypeError("Unexpected key word arguments: {}".format(", ".join(kwargs.keys())))
        # TODO: Checksum with scalems utility, don't just use native Python hash.
        uid = hash(item)
        if uid in manager.tasks:
            # TODO: Consider whether this is the correct behavior
            return manager.tasks[uid]
        else:
            manager.tasks[uid] = item
            return manager.tasks[uid]

    return director


@dataclasses.dataclass(frozen=True)
class SubprocessResult:
    # file: Field(Path)
    # exitcode: Field(int)
    # TODO: Can we use None instead of os.devnull to indicate non-presence of stdout/stderr?
    exitcode: int
    stdout: Path
    stderr: Path
    file: typing.Mapping[str, Path]


class SubprocessTask:
    """Describe the type of resource provided by a Subprocess command."""

    @classmethod
    def scoped_identifier(cls):
        # TODO: Consider either deriving from the `import` identifier,
        #       or defining a more sophisticated protocol for name resolution.
        return ("scalems", "subprocess", "SubprocessTask")

    @classmethod
    def identifier(cls):
        return ".".join(cls.scoped_identifier())

    @classmethod
    def input_type(cls) -> type:
        return SubprocessInput

    @classmethod
    def result_type(cls) -> type:
        return SubprocessResult


# TODO: helpers / ABCs for Serializeable and Encodable.


# TODO: Instances must have a way to map to the owning Context and a task implementation.
# It is reasonable to allow the WorkflowContext to produce an object satisfying a common
# ItemView interface, plus additional interface aspects as specified by the workflow item
# developer in e.g. SubprocessResourceType.
class Subprocess:
    @classmethod
    def resource_type(cls):
        # Note: we return an instance to better reflect the documented object
        # model and to allow for future contextual information, such as shape.
        return SubprocessTask()

    # TODO: Remove uid parameter. It should be calculated.
    def __init__(self, input: SubprocessInput, uid=None):
        self._bound_input = input
        self._result = None
        self._uid = uid
        if self._uid is None:
            self._uid = next_monotonic_integer().to_bytes(32, "big")

    def input_collection(self):
        return self._bound_input

    def result(self):
        return self._result

    def dependencies(self):
        ...

    def uid(self):
        return self._uid

    def serialize(self) -> str:
        """Encode the task as a JSON record.

        Input and Result will be serialized as references.
        The caller is responsible for serializing existing records
        for bound objects, if they exist.
        """
        record = {}
        record["uid"] = self.uid().hex()
        # "label" not yet supported.
        record["type"] = self.resource_type().scoped_identifier()
        record["input"] = dataclasses.asdict(self._bound_input)  # reference
        record["result"] = dataclasses.asdict(self._result)  # reference
        try:
            serialized = json.dumps(record, default=encode)
        except TypeError as e:
            logger.critical("Missing encoding logic for scalems data. Encoder says " + str(e))
            raise InternalError("Missing serialization support.") from e

        # raise MissingImplementationError('To do...')
        return serialized

    @classmethod
    def deserialize(cls, record: str, context=None):
        """Instantiate a Subprocess Task from a serialized record.

        In general, records should only be deserialized into a WorkflowContext
        that manages a valid work graph, but for early testing, at least,
        we have some standalone use cases.
        """

        # The record may or may not have a bound result.
        # If there is a bound result, it should be added to the workgraph first.
        # return cls()
        raise MissingImplementationError()

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
    #     context = scalems.context.get_scope()
    #     # TODO: dispatching
    #     if isinstance(context, scalems.local.LocalExecutor):
    #         from scalems.local.operations import executable as local_exec
    #         # Note that we need a more sophisticated coroutine object than what we get
    #         # directly from `async def` for command instances that can present output
    #         # in multiple contexts or be transferred from one to another.
    #         self._result = local_exec(self)
    #     elif isinstance(context, scalems.radical.RPExecutor):
    #         from scalems.radical.operations import executable as _rp_exec
    #         self._result = _rp_exec(self)
    #     else:
    #         raise MissingImplementationError(
    #         'Current context {} does not implement scalems.executable'.format(context))
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
    #     # of the yield expression
    #     # (e.g. https://docs.python.org/3/howto/functional.html#passing-values-into-a-generator
    #     # but not that the coroutine protocol is slightly different, per https://www.python.org/dev/peps/pep-0492/)
    #     # For instance, this could be a mechanism for nesting event loops or dispatching contexts
    #     # while maintaining a heart-beat or other command-channel-like wrapper.
    #
    #     if not isinstance(self._result, SubprocessResult):
    #         raise RuntimeError('Result was not delivered!')
    #     return self._result


# Register a director for Subprocess workflow items.
# TODO: Wrap this in the decorator or metaclass used for TaskTypes.
@scalems.workflow.workflow_item_director_factory.register
def _(item: Subprocess, *, manager: scalems.workflow.WorkflowManager, label: str = None):
    if not isinstance(manager, scalems.workflow.WorkflowManager):
        raise APIError(f"No director for {repr(manager)}")

    def director(*args, **kwargs):
        if len(args) > 0:
            # TODO: Reconsider reasonable exceptions.
            raise TypeError("Unexpected positional arguments.")
        if len(kwargs) > 0:
            raise TypeError("Unexpected key word arguments: {}".format(", ".join(kwargs.keys())))

        # Note regarding registering task implementation functions:
        # scalems.subprocess is a very special kind of task. Each execution
        # environment needs to provide a specialized implementation for the
        # foreseeable future. It does not make sense for this module to provide
        # a default Python function reference for a task factory or callable.
        task_view = manager.add_item(item)

        return task_view

    return director


def executable(*args, manager: scalems.workflow.WorkflowManager = None, **kwargs):
    """Execute a command line program.

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

    Arguments:
         manager: Workflow manager to which the work should be submitted.
         args: a tuple (or list) to be the subprocess arguments, including the executable

    *args* is required. Additional key words are optional.

    Other Parameters:
         outputs (Mapping): labeled output files, mapping command line flag to one (or
                            more) filenames.
         inputs (Mapping): labeled input files, mapping command line flag to one (or
                           more) filenames.
         environment (Mapping): environment variables to be set in the process
                                environment.
         stdin (str): source for posix style standard input file handle (default None).
         stdout (str): Capture standard out to a filesystem artifact, even if it is not
                       consumed in the workflow.
         stderr (str): Capture standard error to a filesystem artifact, even if it is
                       not consumed in the workflow.
         resources (Mapping): Name additional required resources, such as an MPI
                              environment.

    .. todo:: Support POSIX sigaction / IPC traps?

    .. todo:: Consider dataclasses.dataclass types to replace reusable/composable
              function signatures.

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
    * gpus_per_task (int): Number of GPU devices to allocate for and instance of the
      *exec*.
    * launcher (str): Task launch mechanism, such as ``mpiexec``.

    Returns:
        Output collection contains *exitcode*, *stdout*, *stderr*, *file*.

    The *file* output has the same keys as the *outputs* key word argument.

    Example:
        Execute a command named ``exe`` that takes a flagged option for input
        and output file names
        (stored in a local Python variable ``my_filename`` and as the string literal
        ``'exe.out'``)
        and an ``origin`` flag
        that uses the next three arguments to define a vector.

            >>> my_filename = "somefilename"
            >>> command = scalems.executable(
            ...    ('exe', '--origin', 1.0, 2.0, 3.0),
            ...    inputs={'--infile': scalems.file(my_filename)},
            ...    outputs={'--outfile': scalems.file('exe.out')})
            >>> assert hasattr(command, 'file')
            >>> import os
            >>> assert os.path.exists(command.file['--outfile'].result())
            >>> assert hasattr(command, 'exitcode')

    TODO:
        Consider input/output files that do not appear on the command line, but which
        must figure into data flow.

    """
    if manager is None:
        manager = scalems.workflow.get_scope()

    # TODO: Figure out a reasonable way to check and catch invalid input
    #  through a dispatcher.

    # subprocess_input = context.add(Subprocess.input_type(), *args, **kwargs)
    input_type = Subprocess.resource_type().input_type()
    if not isinstance(input_type, type):
        raise InternalError(
            "Bug: {} is not coded correctly for the {}.input_type() interface.".format(
                __name__, str(type(Subprocess.resource_type()))
            )
        )

    # TODO: Add input separately. First, just add the Subprocess object.
    # Note: static type checkers may not be able to resolve that `input_type is
    # SubprocessInput` for argument checking. Provide local object to the context and
    # replace local reference with a view to the workflow item.
    # subprocess_input = _context.add_to_workflow(
    #     context, Subprocess.resource_type().input_type(), *args, **kwargs)
    bound_input = SubprocessInput(*args, **kwargs)

    director = scalems.workflow.workflow_item_director_factory(Subprocess, manager=manager)
    # Design note: at some point, dynamic workflows will require thread-safe
    # workflow editing context. We could either block on acquiring the editor
    # context, use an async context manager, or hide the possible async
    # aspect by letting the return value of the director be awaitable.

    # TODO: This would be more readable in a form like
    #  `workflow.add_item(Subprocess, bound_input)`

    try:
        task_view: scalems.workflow.ItemView = director(input=bound_input)
    except TypeError as e:
        logger.error("Invalid input in SubprocessInput: " + str(e))
        raise
    except json.JSONDecodeError as e:
        logger.critical("Malformed data: " + e.msg)
        raise InternalError("Bug: internal data is not being conditioned properly") from e
    except Exception as e:
        logger.critical("Unhandled " + repr(e))
        raise

    return task_view
