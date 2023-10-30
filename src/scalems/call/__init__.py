"""Support command line execution of scalems packaged function calls.

Example:
    python -m scalems.call record.json

Note that this whole module is a workaround to avoid using RP raptor.
:file:`scalems/call/__main__.py` provides command line behavior for a
`radical.pilot.Task` *executable* and *arguments*, with which we execute a
serialized representation of a function call, pending restoration of full
"raptor" support.

We don't really need to reconcile the divergent notions of "Subprocess",
if we can remove the need for this entry point.
But we _do_ need to formalize our serialized representation of a function call,
and the task lifecycle and artifacts management.
"""
__all__ = ("CallResult", "main", "cli", "serialize_call", "deserialize_result")

import dataclasses
import functools
import json

# See __main__.py for the entry point executed for command line invocation.

import logging
import os
import pathlib
import sys
import tempfile
import typing
import warnings

import dill

import scalems.store as _store

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclasses.dataclass
class _Call:
    """Record of a function call to be dispatched."""

    func: typing.Callable
    """Python callable.

    Assumed to be round-trip serializable via *encoder* and *decoder*.
    """

    environment: typing.Optional[dict] = dataclasses.field(default_factory=dict)
    """Optional overrides of environment variables."""

    skeleton: typing.Optional[str] = None
    """Optional string-encoded URI for archived skeleton of working directory files."""

    args: typing.Sequence = dataclasses.field(default_factory=tuple)
    """Positional arguments.

    Values are assumed to be round-trip serializable via *encoder* and *decoder*.
    """

    kwargs: dict = dataclasses.field(default_factory=dict)
    """Dictionary of key word arguments and values.

    Values are assumed to be round-trip serializable via *encoder* and *decoder*.
    """

    outputs: list[str] = dataclasses.field(default_factory=list)
    """Requested outputs.

    To avoid excessive data transfer, outputs (ResultPack fields)
    must be named explicitly to be included in the automatically
    generated result package.
    """

    requirements: dict[str, typing.Any] = dataclasses.field(default_factory=dict)
    """Named run time requirements for the call.

    Common requirements include numbers of MPI ranks, cores per process, or
    descriptions of accelerator hardware (GPUs).

    For the initial implementation, key/value pairs are assumed to map to
    attribute assignments in a `radical.pilot.TaskDescription`. The information
    is not provided directly to the function; it is for internal use.
    (Special handling may occur in `scalems.call`.)
    """

    decoder: str = dataclasses.field(default="dill")
    encoder: str = dataclasses.field(default="dill")
    # TODO: Describe return value type.


@dataclasses.dataclass
class CallResult:
    """Result type (container) for a dispatched function call."""

    return_value: typing.Optional[typing.Any] = None
    """Return value, if any.

    Value assumed to be round-trip serializable via *encoder* and *decoder*.
    """

    exception: typing.Optional[str] = None
    """string representation of the Exception, if any."""

    # Side effects
    stdout: typing.Optional[str] = None
    """string-encoded URI for file holding captured stdout."""

    stderr: typing.Optional[str] = None
    """string-encoded URI for file holding captured stderr."""

    directory: typing.Optional[str] = None
    """string-encoded URI for archive of the working directory after task execution."""

    # support
    decoder: str = dataclasses.field(default="dill")
    encoder: str = dataclasses.field(default="dill")


class CallPack(typing.TypedDict):
    """Trivially serializable representation of `scalems.call.Call`.

    Pack and unpack with `scalems.pack.pack_call` and `scalems.pack.unpack_call`.

    Serialize and deserialize with::

        serialized = json.dumps(dataclasses.asdict(callpack), separators=(",", ":"))
        callpack = CallPack(*json.loads(serialized))

    """

    args: list[str]
    """List of serialized positional arguments."""

    environment: typing.Optional[dict]
    """Optional overrides of environment variables."""

    func: str
    """string-encoded Python callable, deserializable with *decoder*.

    If *decoder* consumes *bytes* instead of *str*, *callable* is interpreted
    as hexadecimal-encoded big-endian bytes.
    """

    kwargs: dict[str, str]
    """Dictionary of key word arguments and serialized values."""

    outputs: list[str]
    """Requested outputs.

    To avoid excessive data transfer, outputs (ResultPack fields other than
    *exception*, *encoder*, and *decoder*)
    must be named explicitly to be included in the automatically
    generated result package.
    """

    requirements: dict[str, typing.Any]
    """Run time requirements.

    This is a copy of the run time requirements submitted with the command. It
    is not directly available to the wrapped function, but its information may
    be used by the `scalems.call.main` implementation.

    See Also:
        https://github.com/SCALE-MS/scale-ms/discussions/302
    """

    skeleton: typing.Optional[str]
    """Optional string-encoded URI for archived skeleton of working directory files.

    Warning:
        With or without this file list, additional files will be created to support
        task execution. If this is a problem, we might be able to customize the RP
        task launch script to execute in a subdirectory.
    """

    decoder: str
    encoder: str


class ResultPack(typing.TypedDict):
    """Trivially serializable representation of `scalems.call.CallResult`.

    Serialize and deserialize with::

        serialized = json.dumps(dataclasses.asdict(resultpack), separators=(",", ":"))
        resultpack = CallPack(*json.loads(serialized))

    """

    return_value: typing.Optional[str]
    """string-encoded return value, if any."""

    exception: typing.Optional[str]
    """string representation of the Exception, if any."""

    # Side effects
    stdout: typing.Optional[str]
    """string-encoded URI for file holding captured stdout."""

    stderr: typing.Optional[str]
    """string-encoded URI for file holding captured stderr."""

    directory: typing.Optional[str]
    """string-encoded URI for the working directory detected during task execution."""

    # support
    decoder: str
    encoder: str


@dataclasses.dataclass(frozen=True)
class _Subprocess:
    """Simplified Subprocess representation.

    This exists to support initial implementation and testing of function_call_to_subprocess().
    To be reconciled with generalized Subprocess class after testing and feedback.
    """

    # Note: we can/should enforce uniqueness semantics.
    uid: str
    input_filenames: typing.Mapping[str, _store.FileReference]
    output_filenames: tuple
    executable: str
    arguments: tuple[str, ...]
    requirements: dict = dataclasses.field(default_factory=dict)


async def function_call_to_subprocess(
    func: typing.Callable, *, label: str, args: tuple = (), kwargs: dict = None, datastore, requirements: dict = None,
        venv: str = None,
) -> _Subprocess:
    """
    Wrap a function call in a command line based on `scalems.call`.

    Args:
        func: A callable Python object.
        label: Name (prefix) for identifying tasks and artifacts.
        args: Positional arguments for *func*.
        kwargs: Key word arguments for *func*.
        manager: workflow manager instance.
        requirements: run time requirements (passed through to execution backend).

    Returns:
        Serializable representation of the executable task.

    Warnings:
        This is a temporary utility while we explore use cases and prepare to migrate
        back to :py:mod:`radical.pilot.raptor`. A user-facing tool should return a
        view on a workflow item, whereas this function produces even lower-level details.

    See Also:
        * :py:func:`scalems.radical.runtime.subprocess_to_rp_task()`
        * https://github.com/SCALE-MS/scale-ms/discussions/302

    """
    if kwargs is None:
        kwargs = dict()
    if requirements is None:
        requirements = dict()

    # TODO: FileStore should probably provide a new_file() method.
    #   Possibly an overload to get_file_reference() for IOStreams or buffers,
    #   but requires internal support to optimize out redundant fingerprinting
    #   and filesystem operations. I.e. it should be one read, one fingerprint
    #   calculation, one write, and one rename based on the fingerprint. The
    #   support could be in the form of `add_text`, and `add_blob` methods.
    #   Optimization for known binary formats (with potentially local differences)
    #   could be in the form of `add_typed_data`.
    with tempfile.NamedTemporaryFile(mode="w", suffix="-input.json") as tmp_file:
        tmp_file.write(serialize_call(func=func, args=args, kwargs=kwargs, requirements=requirements))
        tmp_file.flush()
        # We can't release the temporary file until the file reference is obtained.
        file_ref = await _store.get_file_reference(pathlib.Path(tmp_file.name), filestore=datastore)

    uid = str(label)
    input_filename = uid + "-input.json"
    # TODO: Collaborate with scalems.call to agree on output filename.
    output_filename = uid + "-output.json"
    if venv:
        executable = os.path.join(venv, "bin", "python")
    else:
        executable = "python3"
    arguments = []
    for key, value in getattr(sys, "_xoptions", {}).items():
        if value is True:
            arguments.append(f"-X{key}")
        else:
            assert isinstance(value, str)
            arguments.append(f"-X{key}={value}")

    # TODO: Validate with argparse, once scalems.call.__main__ has a real parser.
    if "ranks" in requirements:
        arguments.extend(("-m", "mpi4py"))
    # If this wrapper doesn't go away soon, we should abstract this so path arguments
    # can be generated at the execution site.
    arguments.extend(("-m", "scalems.call", input_filename, output_filename))

    return _Subprocess(
        uid=uid,
        input_filenames={input_filename: file_ref},
        output_filenames=(output_filename,),
        executable=executable,
        arguments=tuple(arguments),
        requirements=requirements.copy(),
    )


def main(call: _Call) -> CallResult:
    """Execute the packaged call.

    Return a packaged result.
    """
    func = call.func
    args = call.args
    kwargs = call.kwargs
    outputs = [str(output) for output in call.outputs]
    fields = [field.name for field in dataclasses.fields(CallResult)]
    for output in outputs:
        if output not in fields:
            # TODO: Make sure that warnings and logs go somewhere under RP.
            warnings.warn(f"Unrecognized output requested: {output}")
    if "exception" not in outputs:
        logger.debug("Adding *exception* to outputs for internal use.")
        outputs.append("exception")

    # Note: we are already running inside a RP executable Task by the time we
    # reach this point, so we are relying on call.environment and call.skeleton
    # to have been handled by the caller.
    cwd = pathlib.Path(os.getcwd())
    logger.info(f"scalems.call executing {func} in working directory {cwd}")
    result_fields = dict(directory=cwd.as_uri())

    # Note: For RP Raptor, the MPIWorker will be providing our wrapped function
    # with an mpi4py.MPI.Comm according to the task requirements. For this CLI
    # utility, RP will have used an appropriate launch method, so the parallelization
    # libraries should be able to initialize themselves from the environment.
    # TODO: Check that *e.g. gromacs* properly detects resources as prepared by RP.
    try:
        result_fields["return_value"] = func(*args, **kwargs)
    except Exception as e:
        result_fields["exception"] = repr(e)

    # Note: We don't have *stdout* and *stderr* at this point because
    # they are handled by the work load manager.
    kwargs = {key: value for key, value in result_fields.items() if key in outputs}
    result = CallResult(**kwargs)
    return result


def cli(*argv: str):
    """Command line entry point.

    Invoke with ``python -m scalems.call <args>``

    TODO: Configurable log level.
    """
    logger.setLevel(logging.DEBUG)
    character_stream = logging.StreamHandler()
    character_stream.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    character_stream.setFormatter(formatter)
    logger.addHandler(character_stream)

    file_logger = logging.FileHandler("scalems.call.log")
    file_logger.setLevel(logging.DEBUG)
    file_logger.setFormatter(formatter)
    logging.getLogger("scalems").addHandler(file_logger)

    logger.debug(f"scalems.call got args: {', '.join(str(arg) for arg in argv)}")
    # TODO: Consider an argparse parser for clarity.
    if len(argv) < 3:
        raise RuntimeError("Arguments are required.")
    # argv[0] will be the __main__.py script. Arguments to `call` are at argv[1:]
    call_path = pathlib.Path(argv[1])
    result_path = pathlib.Path(argv[2])
    with open(call_path, "r") as fh:
        call: _Call = deserialize_call(fh.read())
    result: CallResult = main(call)
    with open(result_path, "w") as fh:
        fh.write(serialize_result(result))
    if result.exception is not None:
        logging.error(result.exception)
        return 1
    else:
        return 0


# For transfer size and debuggability, let's start by serializing as little
# as possible. We'll rely on being able to import details in the execution
# environment (accepting whatever version we get locally) and work from there.
to_bytes = functools.partial(dill.dumps, byref=True, recurse=False)


def from_hex(x: str):
    return dill.loads(bytes.fromhex(x))


def serialize_call(func: typing.Callable, *, args: tuple = (), kwargs: dict = None, requirements: dict = None) -> str:
    """Create a serialized representation of a function call.

    This utility function is provided for stability while the serialization
    machinery and the CallPack structure evolve.
    """
    if kwargs is None:
        kwargs = {}
    if requirements is None:
        requirements = {}
    # This might need to be replaced with a dispatching function, for which raw serialization
    # is only a fall-back for functions that aren't decorated/registered scalems Commands.
    # Commands could be implemented for specific Runtime executors, or expressed in terms
    # of importable callables with similarly expressable Input and Result types.
    # Ref: https://github.com/SCALE-MS/scale-ms/issues/33
    serialized_callable: bytes = to_bytes(func)
    pack = CallPack(
        func=serialized_callable.hex(),
        args=[to_bytes(arg).hex() for arg in args],
        kwargs={key: to_bytes(value).hex() for key, value in kwargs.items()},
        decoder="dill",
        encoder="dill",
        outputs=["return_value", "exception", "directory", "stdout", "stderr"],
        skeleton=None,
        environment=None,
        requirements=requirements,
    )
    return json.dumps(pack, separators=(",", ":"))


def deserialize_call(record: str) -> _Call:
    """Deserialize a function call."""
    record_dict: CallPack = json.loads(record)
    assert record_dict["encoder"] == "dill"
    assert record_dict["decoder"] == "dill"
    # TODO: Pre-load references.
    func = dill.loads(bytes.fromhex(record_dict["func"]))
    args = tuple(from_hex(arg) for arg in record_dict["args"])
    kwargs = {key: from_hex(value) for key, value in record_dict["kwargs"].items()}
    outputs = record_dict["outputs"]
    call = _Call(
        func=func,
        args=args,
        kwargs=kwargs,
        outputs=outputs,
        environment=record_dict.get("environment", None),
        skeleton=record_dict.get("skeleton", None),
        requirements=record_dict.get("requirements", None),
    )
    return call


def serialize_result(result: CallResult) -> str:
    assert result.encoder == "dill"
    assert result.decoder == "dill"
    if result.return_value is not None:
        value = to_bytes(result.return_value).hex()
    else:
        value = None
    if result.exception is not None:
        exception = to_bytes(result.exception).hex()
    else:
        exception = None

    # In scalems.call.cli, we are not managing stdout and stderr redirection.
    # If the caller is managing stdout and stderr, the caller can repackage the
    # result with appropriate URIs.
    pack = ResultPack(
        return_value=value,
        exception=exception,
        stderr=None,
        stdout=None,
        directory=result.directory,
        encoder="dill",
        decoder="dill",
    )
    return json.dumps(pack, separators=(",", ":"))


def deserialize_result(stream: str) -> CallResult:
    pack: ResultPack = json.loads(stream)
    assert pack["encoder"] == "dill"
    assert pack["decoder"] == "dill"

    value = pack.get("return_value", None)
    if value is not None:
        assert isinstance(value, str)
        value = from_hex(value)
    exception = pack.get("exception", None)
    if exception is not None:
        assert isinstance(exception, str)
        exception = from_hex(exception)
    result = CallResult(
        return_value=value,
        exception=exception,
        stdout=pack.get("stdout", None),
        stderr=pack.get("stderr", None),
        directory=pack.get("directory", None),
        decoder=pack["decoder"],
        encoder=pack["encoder"],
    )
    return result
