import collections
import json
import unittest


class Fingerprint(object):
    import hashlib as _hashlib

    def __init__(self, *, operation, input, depends=()):

        # TODO: replace (list, tuple) with abstraction for valid operation values
        if not isinstance(operation, (list, tuple)):
            raise ValueError('Fingerprint requires a sequence of operation name components.')
        else:
            self.operation = tuple(operation)

        # TODO: replace (dict, str) with abstraction for valid input values.
        if not isinstance(input, (dict, str)):
            raise ValueError('Fingerprint requires a valid input representation.')
        else:
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


class Integer64(object):
    import json as _json
    # TODO: Replace numpy dependency with memoryview manager or core gmxapi
    #  buffer protocol provider.
    # Note that the built-in Python array module only provides 1-dimensional arrays.
    from numpy import array as _array

    def __init__(self, data):
        self.data = Integer64._array(data, dtype='int64')

    def to_json(self, **json_args) -> str:
        record = {
            'operation': ('gmxapi', 'Integer64'),
            'input': {'data': self.data.tolist()},
            'depends': ()
        }
        serialization = Integer64._json.dumps(record, **json_args)
        return serialization

    @classmethod
    def from_json(cls, serialized: str):
        record = Integer64._json.loads(serialized)
        # if not record['operation']

    def fingerprint(self):
        return Fingerprint(operation=['gmxapi', 'Integer64'], input={'data': self.data.tolist()})


class Integer64TestCase(unittest.TestCase):
    def test_uid(self):
        # Test fingerprinting for Integer64
        import hashlib

        my_array = Integer64([[1, 2], [3, 4]])
        expected_json = '{"depends":[],"input":{"data":[[1,2],[3,4]]},"operation":["gmxapi","Integer64"]}'

        fingerprint = my_array.fingerprint()

        actual_json = fingerprint.compact_json()
        assert expected_json == actual_json

        expected_hash = hashlib.sha256(expected_json.encode('utf-8')).digest()
        actual_hash = my_array.fingerprint().uid()
        assert expected_hash == actual_hash
