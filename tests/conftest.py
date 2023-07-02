"""Configuration for pytest tests.

Several custom command line options are added to the pytest configuration,
but they must be provided *after* all standard pytest options.

Note: https://docs.python.org/3/library/devmode.html#devmode may be enabled
"using the -X dev command line option or by setting the PYTHONDEVMODE environment
variable to 1."

Note: Enable more radical.pilot debugging information by exporting
RADICAL_LOG_LVL=DEBUG before invocation.
"""
import asyncio

import pytest_asyncio

import scalems.radical.configuration
import scalems.radical.manager
import scalems.radical.session

try:
    # Import radical.pilot early because of interaction with the built-in logging module.
    import radical.pilot as rp
    import radical.utils as ru
except ImportError:
    # It is not an error to run tests without RP, but when RP is available, we
    # need to import it before pytest imports the logging module.
    rp = None
    ru = None

import logging
import os
import pathlib
import shutil
import tempfile
import warnings
from contextlib import contextmanager

import pytest

import scalems.radical
import scalems.radical.runtime
from scalems.context import cwd_lock
from scalems.context import scoped_chdir

logger = logging.getLogger("pytest_config")
logger.setLevel(logging.DEBUG)


def pytest_addoption(parser):
    """Add command-line user options for the pytest invocation."""
    # Automatic venv handling still needs some work. We require the user to
    # explicitly name a resource (defined in
    # $HOME/.radical/pilot/configs/resource_<name>.json)
    # with virtenv details that are known to be valid.
    parser.addoption("--rp-resource", type=str, help="Specify a *resource* for the radical.pilot.PilotDescription.")
    parser.addoption(
        "--rp-access", type=str, help="Explicitly specify the access_schema to use from the RADICAL resource."
    )
    # Automatic venv handling still needs some work. We require the user to explicitly
    # assert that a venv is available.
    # Warning: This venv should also exist on the target resource!
    parser.addoption("--rp-venv", type=str, help="Full path to a pre-configured venv to use for RP tasks.")
    parser.addoption(
        "--pycharm",
        action="store_true",
        default=False,
        help="Attempt to connect to PyCharm remote debugging system, where appropriate.",
    )
    parser.addoption(
        "--rm-tmp",
        type=str,
        default="always",
        help='Remove temporary test working directory "always", "never", or on "success".',
    )
    parser.addoption("--experimental", action="store_true", default=False, help="run tests for experimental features")
    parser.addoption(
        "--exhaustive", action="store_true", default=False, help="run exhaustive coverage with extra tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "experimental: mark test as experimental")
    config.addinivalue_line("markers", "exhaustive: mark test to run only for exhaustive testing")


def pytest_collection_modifyitems(config, items):
    # Ref https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option  # noqa: E501
    skip_experimental = pytest.mark.skip(reason="only runs for --experimental")
    skip_exhaustive = pytest.mark.skip(reason="use --exhaustive for more exhaustive testing")
    if not config.getoption("--experimental"):
        # skip experimental tests unless --experimental given in cli
        for item in items:
            if "experimental" in item.keywords:
                item.add_marker(skip_experimental)
    if not config.getoption("--exhaustive"):
        for item in items:
            if "exhaustive" in item.keywords:
                item.add_marker(skip_exhaustive)


@pytest.fixture(scope="session", autouse=True)
def pycharm_debug(request):
    """If requested, try to connect to a PyCharm remote debugger at
    host.docker.internal:12345.

    Note: the IDE run configuration must be started before launching pytest.
    """
    if request.config.getoption("--pycharm"):
        try:
            import pydevd_pycharm

            return pydevd_pycharm.settrace("host.docker.internal", port=12345, stdoutToServer=True, stderrToServer=True)
        except ImportError:
            ...


@pytest.fixture(scope="session")
def rmtmp(request):
    """Fixture for the --rm-tmp CLI option."""
    choice = request.config.getoption("--rm-tmp")
    if choice not in ("always", "never", "success"):
        raise RuntimeError("Invalid choice for --rm-tmp.")
    return choice


@pytest.fixture(autouse=True)
def propagate_coverage(request):
    """Detect the activity of the pytest-cov plugin.

    The COVERAGE_RUN environment variable provided by :py:mod:`coverage` is not
    set when coverage is managed through the :py:mod:`pytest-cov` plugin. We
    look for the presence of that pytest command line flag and set
    SCALEMS_COVERAGE=TRUE if detected.
    """
    if request.config.getoption("--cov"):
        os.environ["SCALEMS_COVERAGE"] = "TRUE"


@contextmanager
def _cleandir(remove_tempdir: str = "always"):
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

    newpath = tempfile.mkdtemp(prefix="pytest_cleandir_")
    logger.debug(f"_cleandir created {newpath}")

    def remove():
        logger.debug(f"_cleandir removing {newpath}")
        shutil.rmtree(newpath)

    def warn():
        warnings.warn("Temporary directory not removed: {}".format(newpath))

    # Initialize callback function reference
    if remove_tempdir == "always":
        callback = remove
    else:
        callback = warn

    try:
        with scoped_chdir(newpath):
            yield newpath
        logger.debug('_cleandir "with" block successful.')
        # If we get to this line, the `with` block using _cleandir did not throw.
        # Clean up the temporary directory unless the user specified `--rm never`.
        # I.e. If the user specified `--rm success`, then we need to toggle from `warn`
        # to `remove`.
        if remove_tempdir != "never":
            callback = remove
    finally:
        logger.debug("Finalizing _cleandir context.")
        try:
            callback()
        except OSError as e:
            logger.exception("Exception while exiting _cleandir.", exc_info=e)
            logger.error(
                f"{newpath} may not have been successfully removed."
            )  # pytest.exit(msg='Unrecoverable error. Stopping pytest.',  #             returncode=e.errno)
        else:
            logger.debug("_cleandir exited after removing dir.")


@pytest.fixture(scope="function")
def cleandir(rmtmp):
    """Provide a clean temporary working directory for a test.

    WARNING:
        Use sparingly. Some modules launch new threads or processes which could be
        disrupted by the changing working directory associated with this fixture.

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

    Ref:
    https://docs.pytest.org/en/latest/fixture.html#using-fixtures-from-classes-modules-or-projects
    """
    import gc

    with _cleandir(remove_tempdir=rmtmp) as newdir:
        if not cwd_lock.locked():
            raise RuntimeError("Logic error: we released the cwd_lock too early.")
        if not os.path.exists(newdir):
            raise RuntimeError("Logic error. We didn't keep the dir alive long enough.")
        logger.info(f"cleandir context entered: {newdir}")
        yield newdir
        if not os.path.exists(newdir):
            logger.error("Possible logic error: we may have removed the temp dir too soon.")

        # Try to finalize generators before removing temporary directory.
        gc.collect()
        # Note: If there is an exception (or assertion failure), the WorkflowManager and
        # its FileStoreManager may still not get collected, and we may see additional
        # exceptions from the temporary directory getting removed before the FileStore is
        # flushed.

        logger.info("cleandir context is ready to finish. Releasing nested _cleandir.")
    logger.info(f"cleandir left {newdir}. Returned to {os.getcwd()}")


@pytest.fixture(scope="session")
def pilot_description(request) -> rp.PilotDescription:
    """pytest fixture to build PilotDescription from the --rp* CLI options."""
    try:
        import radical.pilot as rp
        import radical.utils as ru

    except ImportError:
        rp = None
        ru = None

    resource = request.config.getoption("--rp-resource")
    if rp is None or ru is None or resource is None or not os.environ.get("RADICAL_PILOT_DBURL"):
        pytest.skip("Test requires RADICAL environment. Provide target resource and " "RADICAL_PILOT_DBURL")

    pilot_description = {"resource": resource, "cores": 4, "gpus": 0, "runtime": 10, "exit_on_error": False}

    access_schema = request.config.getoption("--rp-access")
    if access_schema:
        pilot_description["access_schema"] = access_schema
    pilot_description = rp.PilotDescription(pilot_description)
    return pilot_description


@pytest.fixture(scope="session")
def rp_configuration(request, rp_venv) -> scalems.radical.configuration.RuntimeConfiguration:
    """pytest fixture to configure scalems.radical from CLI options."""
    resource = request.config.getoption("--rp-resource")
    if rp is None or ru is None or resource is None or not os.environ.get("RADICAL_PILOT_DBURL"):
        pytest.skip("Test requires RADICAL environment. Provide target resource and " "RADICAL_PILOT_DBURL")

    access_schema = request.config.getoption("--rp-access")

    job_endpoint: ru.Url = rp.utils.misc.get_resource_job_url(resource, access_schema)
    launch_method = job_endpoint.scheme
    if launch_method == "fork":
        pytest.skip("Raptor is not fully supported with 'fork'-based launch methods.")

    rp_resource_params = {
        "PilotDescription": {
            "access_schema": access_schema,
            "exit_on_error": False,
        }
    }
    config = scalems.radical.configuration.RuntimeConfiguration(
        execution_target=resource,
        target_venv=rp_venv,
        rp_resource_params=rp_resource_params,
        enable_raptor=True,
    )
    return config


@pytest.fixture(scope="session")
def rp_venv(request):
    """pytest fixture to allow a user-specified venv for the RP tasks."""
    path = request.config.getoption("--rp-venv")
    if path is None:
        # pytest.skip('This test only runs for static RP venvs.')
        # return
        warnings.warn("RP tests should be explicitly provided with a venv using --rp-venv.")
    return path


@pytest.fixture(scope="session")
def event_loop():
    """Override the pytest_asyncio event_loop fixture with broader scope.

    The default ``event_loop`` fixture from :py:mod:`pytest_asyncio` has ``function``
    scope, but we would like to be able to re-use runtime resources more broadly.
    Note that the event loop could become unusable if its ThreadPoolExecutor runs
    out of threads from badly behaved code triggered in other tests.

    Ref: https://pytest-asyncio.readthedocs.io/en/latest/reference/fixtures.html
    """
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


# @pytest_asyncio.fixture(scope=event_loop_scope)
# async def async_fixture():
#     return await asyncio.sleep(0.1)


@pytest_asyncio.fixture(scope="session")
async def rp_runtime(rp_configuration, event_loop) -> scalems.radical.session.RuntimeSession:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        runtime = await scalems.radical.session.runtime_session(loop=event_loop, configuration=rp_configuration)
        try:
            yield runtime
        finally:
            runtime.close()
        assert runtime.session.closed


@pytest.fixture(scope="session")
def sdist():
    """Build and provide an sdist of the scalems package in its current state."""
    import build

    src_dir = pathlib.Path(__file__).parent.parent
    assert os.path.exists(src_dir / "tests" / "conftest.py")
    with tempfile.TemporaryDirectory() as dir:
        dist = build.ProjectBuilder(str(src_dir)).build(distribution="sdist", output_directory=dir)
        yield dist
