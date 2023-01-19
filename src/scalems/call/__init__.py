"""Support command line execution of scalems packaged function calls.

Example:
    python -m scalems.call record.json
"""
__all__ = ("Result", "main", "cli", "pack_call", "unpack_result")

import dataclasses
import functools
import json

# See __main__.py for the entry point executed for command line invocation.

import logging
import pathlib
import typing

import dill

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclasses.dataclass
class _Call:
    func: typing.Callable
    """Python callable.

    Assumed to be round-trip serializable via *encoder* and *decoder*.
    """

    environment: dict = dataclasses.field(default_factory=dict)
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

    decoder: str = dataclasses.field(default="dill")
    encoder: str = dataclasses.field(default="dill")
    # TODO: Describe return value type.


@dataclasses.dataclass
class Result:
    return_value: typing.Optional[typing.Any] = None
    """Return value, if any.

    Value assumed to be round-trip serializable via *encoder* and *decoder*.
    """

    # Side effects
    stdout: typing.Optional[str] = None
    """string-encoded URI for file holding captured stdout."""

    stderr: typing.Optional[str] = None
    """string-encoded URI for file holding captured stderr."""

    directory: typing.Optional[str] = None
    """string-encoded URI for archive of the working directory after task execution."""

    exception: typing.Optional[str] = None
    """string representation of the Exception, if any."""

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

    func: str
    """string-encoded Python callable, deserializable with *decoder*.

    If *decoder* consumes *bytes* instead of *str*, *callable* is interpreted
    as hexadecimal-encoded big-endian bytes.
    """

    environment: typing.Optional[dict]
    """Optional overrides of environment variables."""

    skeleton: typing.Optional[str]
    """Optional string-encoded URI for archived skeleton of working directory files."""

    args: list[str]
    """List of serialized positional arguments."""

    kwargs: dict[str, str]
    """Dictionary of key word arguments and serialized values."""

    outputs: list[str]
    """Requested outputs.

    To avoid excessive data transfer, outputs (ResultPack fields)
    must be named explicitly to be included in the automatically
    generated result package.
    """

    decoder: str
    encoder: str


class ResultPack(typing.TypedDict):
    """Trivially serializable representation of `scalems.call.Result`.

    Pack and unpack with `scalems.pack.pack_result` and `scalems.pack.unpack_result`.

    Serialize and deserialize with::

        serialized = json.dumps(dataclasses.asdict(resultpack), separators=(",", ":"))
        resultpack = CallPack(*json.loads(serialized))

    """

    return_value: typing.Optional[str]
    """string-encoded return value, if any."""

    # Side effects
    stdout: typing.Optional[str]
    """string-encoded URI for file holding captured stdout."""

    stderr: typing.Optional[str]
    """string-encoded URI for file holding captured stderr."""

    directory: typing.Optional[str]
    """string-encoded URI for archive of the working directory after task execution."""

    exception: typing.Optional[str]
    """string representation of the Exception, if any."""

    # support
    decoder: str
    encoder: str


def main(call: _Call) -> Result:
    """Execute the packaged call.

    Return a packaged result.
    """
    func = call.func
    args = call.args
    kwargs = call.kwargs
    # TODO: execution environment
    try:
        output = func(*args, **kwargs)
    except Exception as e:
        exception = repr(e)
        result = Result(exception=exception)
    else:
        result = Result(return_value=output)
        # TODO: file outputs
    return result


def cli(*argv):
    """Command line entry point.

    Invoke with ``python -m scalems.call <args>``
    """
    logger.debug(f"scalems.call got args: {', '.join(str(arg) for arg in argv)}")
    # argv[0] will be the __main__.py script. Arguments to `call` are at argv[1:]
    if len(argv) == 0:
        raise RuntimeError("Arguments are required.")
    args = argv[1:]
    package_path = pathlib.Path(args[0])
    with open(package_path, "r") as fh:
        call: _Call = unpack_call(fh.read())
    result = main(call)
    # TODO: Add output file descriptions to result before packaging.
    with open("scalems_out.json", "w") as fh:
        fh.write(pack_result(result))
    return 0


# For transfer size and debuggability, let's start by serializing as little
# as possible. We'll rely on being able to import details in the execution
# environment (accepting whatever version we get locally) and work from there.
to_bytes = functools.partial(dill.dumps, byref=True, recurse=False)


def from_hex(x: str):
    return dill.loads(bytes.fromhex(x))


def pack_call(func: typing.Callable, *, args: tuple = (), kwargs: dict = None) -> str:
    """Create a serialized representation of a function call.

    This utility function is provided for stability while the serialization
    machinery and the CallPack structure evolve.
    """
    if kwargs is None:
        kwargs = {}
    serialized_callable: bytes = to_bytes(func)
    pack = CallPack(
        func=serialized_callable.hex(),
        args=[to_bytes(arg).hex() for arg in args],
        kwargs={key: to_bytes(value).hex() for key, value in kwargs.items()},
        decoder="dill",
        encoder="dill",
        outputs=["return_value"],
        skeleton=None,
        environment=None,
    )
    return json.dumps(pack, separators=(",", ":"))


def unpack_call(record: str) -> _Call:
    """Deserialize a function call."""
    record_dict: CallPack = json.loads(record)
    assert record_dict["encoder"] == "dill"
    assert record_dict["decoder"] == "dill"
    # TODO: Pre-load references.
    func = dill.loads(bytes.fromhex(record_dict["func"]))
    args = tuple(from_hex(arg) for arg in record_dict["args"])
    kwargs = {key: from_hex(value) for key, value in record_dict["kwargs"].items()}
    outputs = record_dict["outputs"]
    call = _Call(func=func, args=args, kwargs=kwargs, outputs=outputs)
    return call


def pack_result(result: Result) -> str:
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

    pack = ResultPack(
        return_value=value,
        exception=exception,
        stdout=None,
        stderr=None,
        directory=None,
        encoder="dill",
        decoder="dill",
    )
    return json.dumps(pack, separators=(",", ":"))


def unpack_result(stream: str) -> Result:
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
    result = Result(
        return_value=value,
        exception=exception,
        stdout=pack.get("stdout", None),
        stderr=pack.get("stderr", None),
        directory=pack.get("directory", None),
        decoder=pack["decoder"],
        encoder=pack["encoder"],
    )
    return result
