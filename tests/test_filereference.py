"""Tracking filesystem objects across workflow environments."""
import asyncio
import contextlib
import hashlib
import locale
import mmap
import os
import pathlib
import sys
import tempfile
import urllib.parse

import pytest
import scalems.context
import scalems.exceptions
import scalems.file
import scalems.store
from scalems.identifiers import ResourceIdentifier

sample_text = ("Hi there!", "Hello, World.")

# Get 8 bytes of sample binary data.
sample_value = int(42)
sample_data = sample_value.to_bytes(length=8, byteorder=sys.byteorder)
assert int.from_bytes(sample_data, byteorder=sys.byteorder) == sample_value


@contextlib.contextmanager
def text_file(newline=None):
    # Create sample input file.
    # Get a temp file name.
    with tempfile.NamedTemporaryFile() as f:
        path = pathlib.Path(f.name)
    assert path.is_absolute()
    assert not path.exists()

    try:
        with path.open(mode="w", newline=newline) as fh:
            for line in sample_text:
                fh.write(line)
                fh.write("\n")
        # Check that the file contents are what we expect.
        with open(path, "r") as fh:
            assert all([sample == read.rstrip() for sample, read in zip(sample_text, fh)])
        yield path
    finally:
        # Make sure that the FileStore is now the only source of the file.
        path.unlink()


@contextlib.contextmanager
def binary_file():
    # Create sample input file.
    fd, filename = tempfile.mkstemp(text=True)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(sample_data)
        # Check that the file contents are what we expect.
        with open(filename, "rb") as fh:
            data: bytes = fh.read()
            assert len(data) == len(sample_data)
            assert data == sample_data
            assert int.from_bytes(data, byteorder=sys.byteorder) == sample_value
            yield filename
    finally:
        # Make sure that the FileStore is now the only source of the file.
        os.unlink(filename)


def test_hash():
    """Check our checksum strategy for logic errors."""
    data1 = sample_value.to_bytes(length=1, byteorder=sys.byteorder)
    data8 = sample_data
    data7 = int.from_bytes(sample_data, byteorder=sys.byteorder).to_bytes(
        length=len(sample_data) - 1, byteorder=sys.byteorder
    )
    assert int.from_bytes(data1, byteorder=sys.byteorder) == sample_value
    assert int.from_bytes(data7, byteorder=sys.byteorder) == sample_value
    assert int.from_bytes(data8, byteorder=sys.byteorder) == sample_value
    assert data1 != data7
    assert data7 != data8

    null_digest = hashlib.sha256().digest()
    digest7 = hashlib.sha256(data7).digest()
    digest8 = hashlib.sha256(data8).digest()
    assert digest8 != null_digest
    assert digest7 != null_digest
    assert digest7 != digest8

    a = int(0).to_bytes(length=8, byteorder=sys.byteorder)
    b = (int.from_bytes(a, byteorder=sys.byteorder) + 1).to_bytes(length=8, byteorder=sys.byteorder)
    assert a != b

    # Check some assumptions about our hashing of binary and text files.
    native_line_ending = os.linesep
    if native_line_ending == "\n":
        nonnative_line_ending = "\r\n"
    else:
        nonnative_line_ending = "\n"
    with text_file(newline=native_line_ending) as filename:
        text_hash_1 = hashlib.sha256()
        with open(filename, "r") as f:
            for line in f:
                text_hash_1.update(line.encode(locale.getpreferredencoding(False)))
    with text_file(newline=nonnative_line_ending) as filename:
        text_hash_2 = hashlib.sha256()
        with open(filename, "r") as f:
            for line in f:
                text_hash_2.update(line.encode(locale.getpreferredencoding(False)))
    assert text_hash_1.digest() == text_hash_2.digest()
    with text_file(newline=nonnative_line_ending) as filename:
        text_hash_3 = hashlib.sha256()
        with open(filename, "r", newline=native_line_ending) as f:
            for line in f:
                text_hash_3.update(line.encode(locale.getpreferredencoding(False)))
    assert text_hash_1.digest() != text_hash_3.digest()

    with text_file(newline=native_line_ending) as filename:
        binary_hash = hashlib.sha256()
        with open(filename, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as data:
                binary_hash.update(data)
    assert binary_hash.digest() == text_hash_1.digest()
    assert binary_hash.digest() == text_hash_2.digest()
    assert binary_hash.digest() != text_hash_3.digest()


@pytest.mark.asyncio
async def test_simple_text_file(tmp_path):
    manager = scalems.store.FileStoreManager(directory=tmp_path)
    datastore = manager.filestore()
    with text_file() as filename:
        # Add the file to the file store.
        future = asyncio.ensure_future(datastore.add_file(scalems.file.describe_file(filename, mode="r")))
        await future
        assert future.done()
        fileref: scalems.file.AbstractFileReference = future.result()

        with pytest.raises(scalems.exceptions.DuplicateKeyError):
            await datastore.add_file(scalems.file.describe_file(filename, mode="r"))
        with pytest.raises(scalems.exceptions.DuplicateKeyError):
            await datastore.add_file(scalems.file.describe_file(filename, mode="r"), _name="spam")

    assert not os.path.exists(filename)
    assert pathlib.Path(fileref).exists()

    assert fileref.filestore() is datastore
    assert fileref.key() in datastore.files
    assert fileref.is_local()
    assert await fileref.localize() is fileref
    uri = fileref.as_uri()
    path = urllib.parse.urlparse(uri).path
    assert path == str(pathlib.Path(fileref))

    with open(fileref, "r") as fh:
        assert all([sample == read.rstrip() for sample, read in zip(sample_text, fh)])

    key = fileref.key()
    assert isinstance(key, ResourceIdentifier)
    assert len(key.bytes()) > 0

    # Make sure a small change is caught.
    duplicate = pathlib.Path(fileref).name
    try:
        with open(filename, "w") as fh:
            fh.write("\n".join(sample_text))
        with open(filename, "r") as fh1:
            with text_file() as f:
                with open(f, "r") as fh2:
                    assert not all([line1.encode() == line2.encode() for line1, line2 in zip(fh1, fh2)])
        assert key in datastore.files
        assert pathlib.Path(fileref) in datastore.files.values()
        with pytest.raises(scalems.exceptions.DuplicateKeyError):
            await datastore.add_file(scalems.file.describe_file(filename, mode="r"), _name=duplicate)

        fileref: scalems.file.AbstractFileReference = await datastore.add_file(
            scalems.file.describe_file(filename, mode="r")
        )
        assert fileref.key() != key
        assert pathlib.Path(fileref).name != duplicate
        assert os.fspath(fileref) != path

    finally:
        os.unlink(filename)

    manager.close()


@pytest.mark.asyncio
async def test_simple_binary_file(tmp_path):
    manager = scalems.store.FileStoreManager(directory=tmp_path)
    datastore = manager.filestore()
    with binary_file() as filename:
        # Add the file to the file store.
        future = asyncio.ensure_future(datastore.add_file(scalems.file.describe_file(filename, mode="rb")))
        await future
        assert future.done()
        fileref: scalems.file.AbstractFileReference = future.result()

        with pytest.raises(scalems.exceptions.DuplicateKeyError):
            await datastore.add_file(scalems.file.describe_file(filename, mode="rb"))
    assert not os.path.exists(filename)
    assert pathlib.Path(fileref).exists()

    with open(fileref, "rb") as fh:
        data: bytes = fh.read()
        assert int.from_bytes(data, byteorder=sys.byteorder) == sample_value

    key: ResourceIdentifier = fileref.key()
    assert isinstance(key, ResourceIdentifier)
    assert len(str(key)) > 0

    # Make sure a small change is handled properly.
    duplicate = pathlib.Path(fileref).name
    try:
        new_data = int.from_bytes(sample_data, byteorder=sys.byteorder).to_bytes(
            length=len(sample_data) - 1, byteorder=sys.byteorder
        )
        assert int.from_bytes(new_data, byteorder=sys.byteorder) == sample_value
        with open(filename, "wb") as fh:
            fh.write(new_data)
        # We are not allowed to use the same key for a new file.
        with pytest.raises(scalems.exceptions.DuplicateKeyError):
            await datastore.add_file(scalems.file.describe_file(filename, mode="rb"), _name=duplicate)
        # We are allowed to let the filestore generate a new entry for a new file.
        # The path of the file argument does not matter because we rely on
        # fingerprinting the file contents.
        fileref = await datastore.add_file(scalems.file.describe_file(filename, mode="rb"))
        new_key: ResourceIdentifier = fileref.key()
        assert new_key != key
        assert len(bytes(new_key)) > 0
        # We get an exception if we try to add the same file twice through add_file,
        # but we have a utility function to wrap up the logic for that.
        with pytest.raises(scalems.exceptions.DuplicateKeyError):
            await datastore.add_file(scalems.file.describe_file(filename, mode="rb"))
        recycled = await scalems.store.get_file_reference(pathlib.Path(filename), filestore=datastore)
        assert recycled.key() == new_key
    finally:
        os.unlink(filename)
    del manager
    assert datastore.closed


@pytest.mark.asyncio
async def test_dispatching(tmp_path):
    manager = scalems.store.FileStoreManager(directory=tmp_path)
    datastore = manager.filestore()
    with text_file() as textfile:
        fileref = await scalems.store.get_file_reference(textfile, filestore=datastore, mode="r")
    assert fileref.key() in datastore.files
    with pathlib.Path(fileref).open("r") as fh:
        assert all([sample == read.rstrip() for sample, read in zip(sample_text, fh)])
    del manager
