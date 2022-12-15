"""Test the scalems package data model helpers and metaprogramming utilities.
"""
import json
import logging
import os
import pathlib
import uuid

import pytest

import scalems.workflow
from scalems.exceptions import ProtocolError
from scalems.serialization import BasicSerializable
from scalems.serialization import decode
from scalems.serialization import encode
from scalems.serialization import Shape
from scalems.identifiers import TypeIdentifier

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

record = """{
    "version"= "scalems_workflow_1",
    "types"= {
        "scalems.SubprocessInput" = {
            "schema" = {
                "spec" = "scalems.v0",
                "name" = "DataType"
            },
            "implementation" = ["scalems", "subprocess", "SubprocessInput"],
            "fields" = {
                "argv" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "String"],
                    "shape"= ["constraints.OneOrMore"]
                },
                "inputs" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                },
                "outputs" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                },
                "stdin" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "File"],
                    "shape"= [1]
                },
                "environment" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                },
                "resources" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                }
            }
        },
        "scalems.SubprocessResult" = {
            "schema" = {
                "spec" = "scalems.v0",
                "name" = "DataType"
            },
            "implementation" = ["scalems", "subprocess", "SubprocessResult"],
            "fields" = {
                "exitcode" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Integer"],
                    "shape"= [1]
                },
                "stdout" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "File"],
                    "shape"= [1]
                },
                "stderr" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "File"],
                    "shape"= [1]
                },
                "file" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                }
            }
        },
        "scalems.Subprocess" = {
            "schema" = {
                "spec" = "scalems.v0",
                "name" = "DataType"
            },
            "implementation" = ["scalems", "subprocess", "SubprocessResult"],
            "fields" = {
                "input" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "SubprocessInput"],
                    "shape"= [1]
                },
                "result" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "SubprocessResult"],
                    "shape"= [1]
                }
            }
        },
    },
    "items"= [
        {
            "label"= "input_files",
            "identity"= "832df1a2-1f0b-4024-a4ab-4160717b8a8c",
            "type"= ["scalems", "Mapping"],
            "shape"= [1],
            "data"= {"-i"= ["infile"]}
        },
        {
            "label"= "output_files",
            "identity"= "d19e2734-a23c-42de-88a0-3287d7ca71ac",
            "type"= ["scalems", "Mapping"],
            "shape"= [1],
            "data"= {"-o"= ["outfile"]}
        },
        {
            "label"= "resource_spec",
            "identity"= "34dfc648-27b3-47db-b6a8-a10c9ae58f09",
            "type"= ["scalems", "Mapping"],
            "shape"= [1],
            "data"= {"ncpus"= 8, "launch_method"= ["exec"]}
        },
        {
            "label"= "subprocess_input",
            "identity"= "26c86a70-b407-471c-85ed-c1ebfa52f592",
            "type"= ["scalems", "SubprocessInput"],
            "shape"= [1],
            "data"= {
                "args"= ["myprogram", "--num_threads", "8"],
                "inputs"= "832df1a2-1f0b-4024-a4ab-4160717b8a8c",
                "outputs"= "d19e2734-a23c-42de-88a0-3287d7ca71ac",
                "stdin"= null,
                "environment" = [{}],
                "resources" = "34dfc648-27b3-47db-b6a8-a10c9ae58f09"
            },
        },
        {
            "label"= "command",
            "identity"= "199f214c-98f5-4fdd-929e-42685f8c00d2",
            "type"= ["scalems", "Subprocess"],
            "shape"= [1],
            "data"= {
                "input"= "26c86a70-b407-471c-85ed-c1ebfa52f592",
                "result"= "199f214c-98f5-4fdd-929e-42685f8c00d2"
            }
        }

    ]
}
"""


def test_shape():
    shape = Shape((1,))
    assert isinstance(shape, tuple)
    assert len(shape) == 1
    assert shape == (1,)

    shape = Shape((3, 3))
    assert isinstance(shape, tuple)
    assert len(shape) == 2
    assert shape == (3, 3)

    shape = Shape(Shape((1,)))
    assert isinstance(shape, tuple)
    assert len(shape) == 1
    assert shape == (1,)
    assert shape == Shape((1,))

    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        Shape(1)


def test_resource_type():
    scoped_name = ["scalems", "subprocess", "SubprocessTask"]
    description = scalems.workflow.Description(resource_type=TypeIdentifier(tuple(scoped_name)), shape=(1,))
    assert description.type() == TypeIdentifier(tuple(scoped_name))


def test_encoding_str():
    """Confirm that bare strings are encoded and decoded as expected.

    Note: we may choose not to support bare strings through our serialization module.
    """
    string = "asdf"
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
    # length = len(series)
    # shape = (length,)

    # Test bare native int list.
    serialized = json.dumps(series, default=encode)
    round_trip = json.loads(serialized)
    assert all([a == b for a, b in zip(series, round_trip)])

    # We may expect structured data to be automatically translated to SCALE-MS representation.
    # assert round_trip.shape() == shape
    # assert round_trip.dtype() == ('scalems', 'Integer')

    array = [[1, 1], [2, 1], [8, 9]]
    # shape = (3, 2)
    serialized = json.dumps(array, default=encode)
    round_trip = json.loads(serialized)
    for rowA, rowB in zip(array, round_trip):
        assert all([a == b for a, b in zip(rowA, rowB)])
    # We expect structured data to be automatically translated to SCALE-MS representation.
    # assert round_trip.shape() == shape
    # assert round_trip.dtype() == ('scalems', 'Integer')


def test_encoding_bytes():
    length = 8
    data = (42).to_bytes(length=length, byteorder="big")
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


def test_basic_decoding():
    # Let the basic encoder/decoder handle things that look like SCALE-MS objects.
    encoded = {
        "label": None,
        "identity": uuid.uuid4().hex,
        "type": ["test", "Spam"],
        "shape": [1],
        "data": ["spam", "eggs", "spam", "spam"],
    }
    instance = decode(encoded)
    assert type(instance) is BasicSerializable
    shape_ref = Shape((1,))
    assert instance.shape() == shape_ref
    type_ref = TypeIdentifier(("test", "Spam"))
    instance_type = instance.dtype()
    assert instance_type == type_ref

    # Test basic encoding, too.
    assert tuple(instance.encode()["data"]) == tuple(encoded["data"])
    assert instance.encode() == decode(instance.encode()).encode()
    # TODO: Check non-trivial shape.


def test_encoder_registration():
    # Test low-level registration details for object representation round trip.
    # There were some to-dos of things we should check...
    ...

    # Test framework for type creation and automatic registration.
    class SpamInstance(BasicSerializable, base_type=("test", "Spam")):
        ...

    instance = SpamInstance(
        label="my_spam",
        identity=uuid.uuid4().hex,
        dtype=["test", "Spam"],
        shape=(1,),
        data=["spam", "eggs", "spam", "spam"],
    )
    assert not type(instance) is BasicSerializable

    encoded = encode(instance)
    decoded = decode(encoded)
    assert not type(decoded) is BasicSerializable
    assert isinstance(decoded, SpamInstance)

    del instance
    del decoded
    del SpamInstance
    import gc

    gc.collect()
    with pytest.raises(ProtocolError):
        decode(encoded)

    decode.unregister(TypeIdentifier(("test", "Spam")))
    decoded = decode(encoded)
    assert type(decoded) is BasicSerializable


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
