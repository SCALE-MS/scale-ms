"""Test the scalems package data model helpers and metaprogramming utilities.
"""
import json
import os
import pathlib

from scalems.core.support.serialization import encode
from scalems.core.support.serialization import Serializable

record = """{
    "version"= "scalems_workflow_1",
    "types"= {
        "scalems.SubprocessInput" = {
            "argv" = { "type"= ["scalems", "String"], "shape"= ["constraints.OneOrMore"] },
            "inputs" = { "type"= ["scalems", "Mapping"], "shape"= [1] },
            "outputs" = { "type"= ["scalems", "Mapping"], "shape"= [1] },
            "stdin" = { "type"= ["scalems", "File"], "shape"= [1] },
            "environment" = { "type"= ["scalems", "Mapping"], "shape"= [1] },
            "resources" = { "type"= ["scalems", "Mapping"], "shape"= [1] }
        },
        "scalems.SubprocessResult" = {
            "exitcode" = { "type"= ["scalems", "Integer"], "shape"= [1] },
            "stdout" = { "type"= ["scalems", "File"], "shape"= [1] },
            "stderr" = { "type"= ["scalems", "File"], "shape"= [1] },
            "file" = { "type"= ["scalems", "Mapping"], "shape"= [1] }
        },
        "scalems.Subprocess" = {
            "input" = { "type"= ["scalems", "SubprocessInput"], "shape"= [1] },
            "result" = { "type"= ["scalems", "SubprocessResult"], "shape"= [1] }
        },
    },
    "referents"= [
        {
            "label"= "input_files",
            "uid"= "aaaa...",
            "type"= ["scalems", "Mapping"],
            "data"= [{"-i"= ["infile"]}]
        },
        {
            "label"= "output_files",
            "uid"= "bbbb...",
            "type"= ["scalems", "Mapping"],
            "data"= [{"-o"= ["outfile"]}]
        },
        {
            "label"= "resource_spec",
            "uid"= "cccc...",
            "type"= ["scalems", "Mapping"],
            "data"= [{"ncpus"= 8, "launch_method"= ["exec"]}]
        },
        {
            "label"= "subprocess_input",
            "uid"= "dddd...",
            "type"= ["scalems", "SubprocessInput"],
            "args"= ["myprogram", "--num_threads", "8"],
            "inputs"= "aaaa...",
            "outputs"= "bbbb...",
            "stdin"= null,
            "environment" = [{}],
            "resources" = "cccc..."
        },
        {
            "label"= "command",
            "uid"= "eeee...",
            "type"= ["scalems", "Subprocess"],
            "input"= "dddd...",
            "result"= "eeee..."
        }

    ]
}
"""


class BasicSerializable:
    ...


def test_encoding_str():
    """Confirm that bare strings are encoded and decoded as expected.

    Note: we may choose not to support bare strings through our serialization module.
    """
    string = 'asdf'
    serialized = json.dumps(string, default=encode)
    round_trip = json.loads(serialized)
    assert string == round_trip


def test_encoding_scalars():
    """Confirm that scalar data is encoded and decoded as expected.

    Note: We may choose not to support certain forms of data for the full round trip, particularly bare scalars.
    """
    scalar = 42
    serialized = json.dumps(scalar, default=encode)
    round_trip = json.loads(serialized)
    assert scalar == round_trip

    scalar = None
    serialized = json.dumps(scalar, default=encode)
    round_trip = json.loads(serialized)
    assert scalar == round_trip

    scalar = True
    serialized = json.dumps(scalar, default=encode)
    round_trip = json.loads(serialized)
    assert scalar == round_trip


def test_encoding_int():
    """Confirm that integer data is encoded and decoded as expected.

    Note: We may choose not to support certain forms of integer data for the full round trip.
    """
    series = [1, 1, 2, 3, 5]
    length = len(series)
    shape = (length,)
    serialized = json.dumps(series, default=encode)
    round_trip = json.loads(serialized)
    assert all([a == b for a, b in zip(series, round_trip)])
    # We expect structured data to be automatically translated to SCALE-MS representation.
    # assert round_trip.shape() == shape
    # assert round_trip.dtype() == ('scalems', 'Integer')

    array = [[1, 1], [2, 1], [8, 9]]
    shape = (3, 2)
    serialized = json.dumps(array, default=encode)
    round_trip = json.loads(serialized)
    for rowA, rowB in zip(array, round_trip):
        assert all([a == b for a, b in zip(rowA, rowB)])
    # We expect structured data to be automatically translated to SCALE-MS representation.
    # assert round_trip.shape() == shape
    # assert round_trip.dtype() == ('scalems', 'Integer')


def test_encoding_bytes():
    length=8
    data = (42).to_bytes(length=length, byteorder='big')
    serialized = json.dumps(data, default=encode)
    round_trip = json.loads(serialized)
    # TODO: decoder
    assert round_trip == data.hex()
    # assert data == bytes(round_trip)

def test_encoding_fileobject():
    import tempfile
    with tempfile.NamedTemporaryFile() as fh:
        filename = fh.name
        assert os.path.exists(filename)
        serialized = json.dumps(filename, default=encode)
        round_trip = json.loads(serialized)
        # TODO: decoder
        assert os.path.exists(round_trip)
        # assert round_trip.dtype() == ('scalems', 'File')
        # assert round_trip.shape() == (1,)
        assert pathlib.Path(filename) == pathlib.Path(round_trip)


def test_encoder_registration():
    # Test low-level registration details for object representation round trip.
    class Spam(int):
        def encode(self):
            representation = {
                'schema': {
                    'spec': 'scalems_0',
                    'name': 'DataType'
                },
                'type': ('test', 'Spam')
            }
            return representation

    # Test framework for type creation and automatic registration.


def test_serialization():
    ...


def test_deserialization():
    # Check for valid JSON
    serialized = json.dumps(record, default=encode)
    assert json.loads(serialized)

    ...


def test_fingerprint():
    ...


def test_object_model():
    """Check that the decorators and annotation types work right.

    Class definitions use decorators and members of special types to generate
    definitions that support the SCALE-MS object model.

    Independently from any static type checking support, this test confirms that
    the utilities actually produce the objects and relationships we expect.
    """
