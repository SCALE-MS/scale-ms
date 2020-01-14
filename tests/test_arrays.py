import unittest

from serialization import Integer64


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
