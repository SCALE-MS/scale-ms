# Import radical.pilot early because of interaction with the built-in logging module.
# TODO: Did this work?
import asyncio
import pathlib
import subprocess

try:
    import radical.pilot as rp
except ImportError:
    # It is not an error to run tests without RP, but when RP is available, we
    # need to import it before pytest imports the logging module.
    ...

import os
import shutil
import tempfile
import warnings
from contextlib import contextmanager
from urllib.parse import urlparse, ParseResult

import pytest

asyncio.get_event_loop().set_debug(True)


# Note: https://docs.python.org/3/library/devmode.html#devmode is enabled
# "using the -X dev command line option or by setting the PYTHONDEVMODE environment variable to 1."

def pytest_addoption(parser):
    """Add command-line user options for the pytest invocation."""
    parser.addoption(
        '--rp-resource',
        action='store',
        default='local.localhost',
        help='Specify a *resource* for the radical.pilot.PilotDescription.'
    )
    parser.addoption(
        '--rp-access',
        action='store',
        type=str,
        help='Explicitly specify the access_schema to use from the RADICAL resource.'
    )
    parser.addoption(
        '--rp-venv',
        action='store',
        type=str,
        help='Full path to a pre-configured venv to use for RP tasks.'
    )
    parser.addoption(
        '--pycharm',
        action='store_true',
        default=False,
        help='Attempt to connect to PyCharm remote debugging system, where appropriate.'
    )


@pytest.fixture(scope='session', autouse=True)
def pycharm_debug(request):
    """If requested, try to connect to a PyCharm remote debugger at host.docker.internal:12345.

    Note: the IDE run configuration must be started before launching pytest.
    """
    if request.config.getoption('--pycharm'):
        try:
            import pydevd_pycharm
            return pydevd_pycharm.settrace('host.docker.internal', port=12345, stdoutToServer=True, stderrToServer=True)
        except ImportError:
            ...


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
def pilot_description(request) -> rp.PilotDescription:
    """pytest fixture to get access to the --rm CLI option."""
    try:
        import radical.pilot as rp
        import radical.utils as ru

    except ImportError:
        rp = None
        ru = None

    if rp is None or ru is None or not os.environ.get('RADICAL_PILOT_DBURL'):
        pytest.skip("Test requires RADICAL environment.")

    resource = request.config.getoption('--rp-resource')
    assert resource is not None
    access_schema = request.config.getoption('--rp-access')

    pilot_description = {
        'resource': resource,
        'cores': 4,
        'gpus': 0,
        'runtime': 600,
        'exit_on_error': False
    }
    if access_schema:
        pilot_description['access_schema'] = access_schema
    pilot_description = rp.PilotDescription(pilot_description)
    return pilot_description


@pytest.fixture(scope='session')
def rp_venv(request):
    """pytest fixture to allow a user-specified venv for the RP tasks."""
    path = request.config.getoption('--rp-venv')
    if path is None:
        pytest.skip('This test only runs for static RP venvs.')
    else:
        return path


@pytest.fixture(scope='session')
def rp_task_manager(pilot_description: rp.PilotDescription) -> rp.TaskManager:
    """Provide a task_manager using the indicated resource."""
    # Note: Session creation will fail with a FileNotFound error unless venv
    #       is explicitly `activate`d (or the scripts installed with RADICAL components
    #       are otherwise made available on the PATH).

    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=DeprecationWarning)
        session = rp.Session()

    resource = session.get_resource_config(pilot_description.resource)

    if pilot_description.access_schema == 'ssh':
        ssh_target = resource['ssh']['job_manager_endpoint']
        result: ParseResult = urlparse(ssh_target)
        assert result.scheme == 'ssh'
        user = result.username
        port = result.port
        host = result.hostname

        ssh = ['ssh']
        if user:
            ssh.extend(['-l', user])
        if port:
            ssh.extend(['-p', str(port)])
        ssh.append(host)

        process = subprocess.run(
            ssh + ['/bin/echo', 'success'],
            stdout=subprocess.PIPE,
            timeout=5,
            encoding='utf-8')
        if process.returncode != 0 or process.stdout.rstrip() != 'success':
            pytest.skip(f'Could not ssh to target computing resource with {" ".join(ssh)}.')
            return

        # Reuse existing venv for stability and speed.
        # TODO: Reconsider or generalize.
        resource.virtenv_mode = 'use'
        resource.virtenv = '/home/rp/rp-venv'
        resource.rp_version = 'installed'
    else:
        if pilot_description.access_schema is None:
            pilot_description.access_schema = 'local'
        assert pilot_description.access_schema == 'local'
        resource.virtenv_mode = 'local'

    # It looks like we should expect add_resource_config to replace existing definitions.
    session.add_resource_config(resource)

    pmgr = rp.PilotManager(session=session)
    pilot = pmgr.submit_pilots(rp.PilotDescription(pilot_description))
    tmgr = rp.TaskManager(session=session)
    tmgr.add_pilots(pilot)
    with session:
        yield tmgr
        pilot.cancel()

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
