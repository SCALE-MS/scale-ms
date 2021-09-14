"""Tracking filesystem objects across workflow environments."""
import asyncio
import os
import tempfile
import urllib.parse

import pytest
import scalems.context
import scalems.context as _context
import scalems.exceptions

sample_text = (
    'Hi there!',
    'Hello, World.')


@pytest.mark.asyncio
async def test_simple_text_file(tmp_path):
    with _context.scoped_chdir(tmp_path):
        with scalems.context.initialize_datastore() as datastore:
            fd, filename = tempfile.mkstemp(text=True)
            try:
                with os.fdopen(fd, 'w') as fh:
                    for line in sample_text:
                        fh.write(line)
                        fh.write('\n')
                with open(filename, 'r') as fh:
                    assert all([sample == read.rstrip() for sample, read in zip(
                        sample_text, fh)])
                future = asyncio.ensure_future(datastore.add_file(filename, mode='r'))
                await future
                assert future.done()
                fileref = future.result()
            finally:
                os.unlink(filename)
            assert not os.path.exists(filename)
            assert fileref.path().exists()

            assert fileref.filestore() is datastore
            assert fileref.key() in datastore.files
            assert fileref.is_local()
            assert await fileref.localize() is fileref
            uri = fileref.as_uri()
            path = urllib.parse.urlparse(uri).path
            assert path == str(fileref.path())

            with open(fileref.path(), 'r') as fh:
                assert all([sample == read.rstrip() for sample, read in zip(
                    sample_text, fh)])

            key = fileref.key()
            assert isinstance(key, str)
            assert len(key) > 0
            # Make sure a small change is caught.
            try:
                with open(filename, 'w') as fh:
                    fh.write('\n'.join(sample_text))
                with pytest.raises(scalems.exceptions.DuplicateKeyError):
                    await datastore.add_file(filename, mode='r', key=key)
                fileref = await datastore.add_file(filename, mode='r')
                new_key = fileref.key()
                assert new_key != key
                assert isinstance(new_key, str)
                assert len(new_key) > 0
            finally:
                os.unlink(filename)

    assert datastore.closed


def test_simple_binary_file(tmp_path):
    with _context.scoped_chdir(tmp_path):
        with scalems.context.initialize_datastore() as datastore:
            with tempfile.TemporaryFile():
                ...
    assert datastore.closed
