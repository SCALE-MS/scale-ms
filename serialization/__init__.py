"""Serialization utilities."""

import collections
import json
import typing


# The behavior of `bytes` is sufficient that a UID class is probably not necessary,
# though we might want to guarantee that a UID is exactly 32 bytes. TBD...

class Fingerprint(object):
    import hashlib as _hashlib

    def __init__(self, *,
                 operation: typing.Sequence,
                 input: typing.Union[str, typing.Mapping],
                 depends: typing.Sequence = ()):

        # TODO: replace (list, tuple) with abstraction for valid operation values
        if not isinstance(operation, (list, tuple)):
            raise ValueError('Fingerprint requires a sequence of operation name components.')
        else:
            self.operation = tuple(operation)

        # TODO: replace (dict, str) with abstraction for valid input values.
        if not isinstance(input, (dict, str)):
            raise ValueError('Fingerprint requires a valid input representation.')
        elif isinstance(input, str):
            # TODO: chase reference
            self.input = str(input)
        else:
            assert isinstance(input, dict)
            self.input = {key: value for key, value in input.items()}

        # TODO: replace (list, tuple) with abstraction for valid depends values.
        if not isinstance(depends, (list, tuple)):
            ValueError('Fingerprint requires a sequence for dependency specification.')
        else:
            self.depends = tuple(depends)

    def compact_json(self):
        identifiers = collections.OrderedDict([
            ('depends', self.depends),
            ('input', self.input),
            ('operation', self.operation)
        ])
        id_string = json.dumps(identifiers, separators=(',', ':'), sort_keys=True, ensure_ascii=True)
        return id_string

    def uid(self) -> bytes:
        id_string = self.compact_json()
        id_bytes = id_string.encode('utf-8')
        id_hash = Fingerprint._hashlib.sha256(id_bytes)
        size = id_hash.digest_size
        if not size == 32:
            raise ValueError('Expected digest_size 8, but got {}'.format(size))
        digest = id_hash.digest()
        assert isinstance(digest, bytes)
        assert len(digest) == size
        return digest


def _random_uid():
    """Generate a random (invalid) UID, such as for testing."""
    import hashlib as _hashlib
    from random import randint
    return _hashlib.sha256(randint(0, 2**255).to_bytes(32, byteorder='big')).digest()


class OperationIdentifier(tuple):
    def namespace(self):
        return tuple(self[0:-2])

    def operation_name(self):
        return self[-1]

    def __str__(self):
        return '.'.join(self)


class Integer64(object):
    import json as _json
    # TODO: Replace numpy dependency with memoryview manager or core gmxapi
    #  buffer protocol provider.
    # Note that the built-in Python array module only provides 1-dimensional arrays.
    from numpy import array as _array

    operation = OperationIdentifier(['gmxapi', 'Integer64'])

    def __init__(self, data):
        # Note: numpy may be too forgiving regarding source data and we may want extra sanitization.
        self.data = Integer64._array(data, dtype='int64')

    def to_json(self, **json_args) -> str:
        record = {
            'operation': Integer64.operation,
            'input': {'data': self.data.tolist()},
            'depends': ()
        }
        serialization = Integer64._json.dumps(record, **json_args)
        return serialization

    @classmethod
    def from_json(cls, serialized: str):
        record = cls._json.loads(serialized)
        for required_key in ['operation', 'input']:
            if required_key not in record:
                raise ValueError('Invalid record received.')
        if tuple(record['operation']) != cls.operation:
            raise ValueError('Not a {} record.'.format(cls.operation))
        if 'data' not in record['input']:
            raise ValueError('Expected "data" input field.')
        try:
            data = cls._array(record['input']['data'], dtype='int64')
            # TODO: Handle exceptions as we figure out what can go wrong.
        except ValueError as e:
            raise ValueError('Could not create {} from data.'.format(cls.operation)) from e
        return cls(data)

    def fingerprint(self):
        return Fingerprint(operation=['gmxapi', 'Integer64'], input={'data': self.data.tolist()})

# TODO: Probably want a generic SerializedOperation named type or abstract handling
#  for the various ways const nodes could be passed.