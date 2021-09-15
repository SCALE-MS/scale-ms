"""Tracking filesystem objects across workflow environments."""
import asyncio
import os
import sys
import tempfile
import urllib.parse

import pytest
import scalems.context
import scalems.context as _context
import scalems.exceptions

sample_text = (
    'Hi there!',
    'Hello, World.')

# Get 8 bytes of sample binary data.
sample_value = int(42)
sample_data = sample_value.to_bytes(length=8, byteorder=sys.byteorder)


@pytest.mark.asyncio
async def test_simple_text_file(tmp_path):
    with _context.scoped_chdir(tmp_path):
        with scalems.context.initialize_datastore() as datastore:
            # Create sample input file.
            fd, filename = tempfile.mkstemp(text=True)
            try:
                with os.fdopen(fd, 'w') as fh:
                    for line in sample_text:
                        fh.write(line)
                        fh.write('\n')
                # Check that the file contents are what we expect.
                with open(filename, 'r') as fh:
                    assert all([sample == read.rstrip() for sample, read in zip(
                        sample_text, fh)])
                # Add the file to the file store.
                future = asyncio.ensure_future(datastore.add_file(filename, mode='r'))
                await future
                assert future.done()
                fileref = future.result()
            finally:
                # Make sure that the FileStore is now the only source of the file.
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


@pytest.mark.asyncio
async def test_simple_binary_file(tmp_path):
    with _context.scoped_chdir(tmp_path):
        with scalems.context.initialize_datastore() as datastore:
            # Create sample input file.
            fd, filename = tempfile.mkstemp(text=True)
            try:
                with os.fdopen(fd, 'wb') as fh:
                    fh.write(sample_data)
                # Check that the file contents are what we expect.
                with open(filename, 'rb') as fh:
                    data: bytes = fh.read()
                    assert len(data) == len(sample_data)
                    assert data == sample_data
                # Add the file to the file store.
                future = asyncio.ensure_future(datastore.add_file(filename, mode='rb'))
                await future
                assert future.done()
                fileref = future.result()
            finally:
                # Make sure that the FileStore is now the only source of the file.
                os.unlink(filename)
            assert not os.path.exists(filename)
            assert fileref.path().exists()

            with open(fileref.path(), 'rb') as fh:
                data: bytes = fh.read()
                assert len(data) == len(sample_data)
                assert data == sample_data

            key = fileref.key()
            assert isinstance(key, str)
            assert len(key) > 0
            # Make sure a small change is caught.
            try:
                new_data = int\
                    .from_bytes(sample_data, byteorder=sys.byteorder)\
                    .to_bytes(
                    length=len(sample_data) - 1,
                    byteorder=sys.byteorder)
                assert int.from_bytes(new_data, byteorder=sys.byteorder) == sample_value
                with open(filename, 'wb') as fh:
                    fh.write(new_data)
                with pytest.raises(scalems.exceptions.DuplicateKeyError):
                    await datastore.add_file(filename, mode='rb', key=key)
                fileref = await datastore.add_file(filename, mode='rb')
                new_key = fileref.key()
                assert new_key != key
                assert isinstance(new_key, str)
                assert len(new_key) > 0
            finally:
                os.unlink(filename)

    assert datastore.closed
