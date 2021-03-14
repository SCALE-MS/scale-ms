# Import radical.pilot early because of interaction with the built-in logging module.
# TODO: Did this work?
import pathlib

try:
    import radical.pilot
except ImportError:
    # It is not an error to run tests without RP, but when RP is available, we
    # need to import it before pytest imports the logging module.
    ...

import os
import shutil
import sys
import tempfile
import warnings
from contextlib import contextmanager

import pytest

# Work around RADICAL assumption that the Python virtual environment root is on the executable search path.
_PATH_orig = str(os.environ['PATH'])
_py_bin_path = os.path.join(sys.exec_prefix, 'bin')
if _py_bin_path not in _PATH_orig.split(os.pathsep):
    os.environ['PATH'] = os.pathsep.join([_py_bin_path, _PATH_orig])


@contextmanager
def scoped_chdir(dir):
    oldpath = os.getcwd()
    os.chdir(dir)
    try:
        yield dir
        # If the `with` block using scoped_chdir produces an exception, it will
        # be raised at this point in this function. We want the exception to
        # propagate out of the `with` block, but first we want to restore the
        # original working directory, so we skip `except` but provide a `finally`.
    finally:
        os.chdir(oldpath)


@contextmanager
def _cleandir(remove_tempdir: str = 'always'):
    """Context manager for a clean temporary working directory.

    Arguments:
        remove_tempdir: whether to remove temporary directory "always",
                        "never", or on "success"

    Raises:
        ValueError: if remove_tempdir value is not valid.

    The context manager will issue a warning for each temporary directory that
    is not removed.
    """
    remove_tempdir = str(remove_tempdir)

    newpath = tempfile.mkdtemp()

    def remove():
        shutil.rmtree(newpath)

    def warn():
        warnings.warn('Temporary directory not removed: {}'.format(newpath))

    # Initialize callback function reference
    if remove_tempdir == 'always':
        callback = remove
    else:
        callback = warn

    try:
        with scoped_chdir(newpath):
            yield newpath
        # If we get to this line, the `with` block using _cleandir did not throw.
        # Clean up the temporary directory unless the user specified `--rm never`.
        # I.e. If the user specified `--rm success`, then we need to toggle from `warn` to `remove`.
        if remove_tempdir != 'never':
            callback = remove
    finally:
        callback()


@pytest.fixture
def cleandir():
    """Provide a clean temporary working directory for a test.

    Example usage:

        import os
        import pytest

        @pytest.mark.usefixtures("cleandir")
        def test_cwd_starts_empty():
            assert os.listdir(os.getcwd()) == []
            with open("myfile", "w") as f:
                f.write("hello")

        def test_cwd_also_starts_empty(cleandir):
            assert os.listdir(os.getcwd()) == []
            assert os.path.abspath(os.getcwd()) == os.path.abspath(cleandir)
            with open("myfile", "w") as f:
                f.write("hello")

        @pytest.mark.usefixtures("cleandir")
        class TestDirectoryInit(object):
            def test_cwd_starts_empty(self):
                assert os.listdir(os.getcwd()) == []
                with open("myfile", "w") as f:
                    f.write("hello")

            def test_cwd_also_starts_empty(self):
                assert os.listdir(os.getcwd()) == []
                with open("myfile", "w") as f:
                    f.write("hello")

    Ref: https://docs.pytest.org/en/latest/fixture.html#using-fixtures-from-classes-modules-or-projects
    """
    with _cleandir() as newdir:
        yield newdir


@pytest.fixture(scope='session')
def rpsession():
    import radical.pilot as rp
    # Note: Session creation will fail with a FileNotFound error unless venv
    #       is explicitly `activate`d (or the scripts installed with RADICAL components
    #       are otherwise made available on the PATH).

    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=DeprecationWarning)
        session = rp.Session()
    with session:
        yield session
    assert session.closed


@pytest.fixture(scope='session')
def sdist():
    """Build and provide an sdist of the scalems package in its current state."""
    import build
    src_dir = pathlib.Path(__file__).parent.parent
    assert os.path.exists(src_dir / 'tests' / 'conftest.py')
    with tempfile.TemporaryDirectory() as dir:
        dist = build.ProjectBuilder(src_dir).build(distribution='sdist', output_directory=dir)
        yield dist
