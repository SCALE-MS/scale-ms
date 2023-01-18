"""Configuration for pytest tests.

Several custom command line options are added to the pytest configuration,
but they must be provided *after* all standard pytest options.

Note: https://docs.python.org/3/library/devmode.html#devmode may be enabled
"using the -X dev command line option or by setting the PYTHONDEVMODE environment
variable to 1."

Note: Enable more radical.pilot debugging information by exporting
RADICAL_LOG_LVL=DEBUG before invocation.
"""

try:
    # Import radical.pilot early because of interaction with the built-in logging module.
    import radical.pilot as rp
except ImportError:
    # It is not an error to run tests without RP, but when RP is available, we
    # need to import it before pytest imports the logging module.
    rp = None

import logging
import os
import pathlib
import shutil
import subprocess
import tempfile
import warnings
from contextlib import contextmanager
from urllib.parse import ParseResult
from urllib.parse import urlparse

import pytest

import scalems.radical
import scalems.radical.runtime
from scalems.context import cwd_lock
from scalems.context import scoped_chdir

# from scalems.radical.runtime import Runtime
import scalems.rp.runtime
from scalems.rp.runtime import Runtime

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
            logger.error(f"{newpath} may not have been successfully removed.")
            # pytest.exit(msg='Unrecoverable error. Stopping pytest.',
            #             returncode=e.errno)
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
    """pytest fixture to get access to the --rm CLI option."""
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
def rp_venv(request):
    """pytest fixture to allow a user-specified venv for the RP tasks."""
    path = request.config.getoption("--rp-venv")
    if path is None:
        # pytest.skip('This test only runs for static RP venvs.')
        # return
        warnings.warn("RP tests should be explicitly provided with a venv using --rp-venv.")
    return path


def _new_session():
    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        session = rp.Session()
        logger.info(f"Created {session.uid}")
    return session


def _new_runtime():
    session = _new_session()
    runtime = Runtime(session)
    return runtime


def _new_pilotmanager(session: rp.Session):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        return rp.PilotManager(session=session)


def _new_taskmanager(session: rp.Session, pilot: rp.Pilot):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        tmgr = rp.TaskManager(session=session)
        tmgr.add_pilots(pilot)
    return tmgr


def _new_pilot(session: rp.Session, pilot_manager: rp.PilotManager, pilot_description: rp.PilotDescription, venv: str):
    # Note: we can't manipulate the resource definition after creating the Session,
    # but we could check whether the resource is using the same venv that the user
    # is requesting.
    if venv is None:
        pytest.skip("This test requires a user-provided static RP venv.")

    resource = session.get_resource_config(pilot_description.resource)

    if pilot_description.access_schema == "ssh":
        ssh_target = resource["ssh"]["job_manager_endpoint"]
        result: ParseResult = urlparse(ssh_target)
        assert result.scheme == "ssh"
        user = result.username
        port = result.port
        host = result.hostname

        ssh = ["ssh"]
        if user:
            ssh.extend(["-l", user])
        if port:
            ssh.extend(["-p", str(port)])
        ssh.append(host)

        process = subprocess.run(
            ssh + ["/bin/echo", "success"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5, encoding="utf-8"
        )
        if process.returncode != 0 or process.stdout.rstrip() != "success":
            logger.error("Failed ssh stdout: " + str(process.stdout))
            logger.error("Failed ssh stderr: " + str(process.stderr))
            pytest.skip(f"Could not ssh to target computing resource with " f'{" ".join(ssh)}.')
            return

    else:
        # Not using ssh access. Assuming 'local'.
        if pilot_description.access_schema is None:
            pilot_description.access_schema = "local"
        assert pilot_description.access_schema == "local"

    logger.debug(
        "Using resource config: {}".format(str(session.get_resource_config(pilot_description.resource).as_dict()))
    )
    logger.debug("Using PilotDescription: {}".format(str(pilot_description.as_dict())))
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        pilot = pilot_manager.submit_pilots(rp.PilotDescription(pilot_description))
    return pilot


@pytest.fixture(scope="session")
def rp_runtime(pilot_description) -> Runtime:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        runtime: Runtime = _new_runtime()
        try:
            yield runtime
        finally:
            scalems.rp.runtime.RPDispatchingExecutor.runtime_shutdown(runtime)
        assert runtime.session.closed


def _check_pilot_manager(runtime: Runtime):
    # Caller should destroy and recreate Pilot if this call has to replace PilotManager.
    session = runtime.session
    original_pilot_manager: rp.PilotManager = runtime.pilot_manager()
    if session.closed:
        logger.info(f"{session.uid} is closed. Creating new Session.")
        session: rp.Session = _new_session()
        runtime.reset(session)
    if original_pilot_manager is None or original_pilot_manager is not runtime.pilot_manager():
        # Is there a way to check whether the PilotManager is healthy?
        logger.info(f"Creating a new PilotManager for {runtime.session.uid}")
        if isinstance(original_pilot_manager, rp.PilotManager):
            logger.info("Closing old PilotManager")
            original_pilot_manager.close()
        pilot_manager = _new_pilotmanager(runtime.session)
        logger.info(f"New PilotManager is {pilot_manager.uid}")
        runtime.pilot_manager(pilot_manager)


def _check_pilot(runtime: Runtime, pilot_description: rp.PilotDescription, venv):
    pilot_manager = runtime.pilot_manager()
    pilot = runtime.pilot()
    _check_pilot_manager(runtime=runtime)
    if runtime.pilot_manager() is not pilot_manager or pilot is None or pilot.state in rp.FINAL:
        if runtime.pilot_manager() is not pilot_manager:
            logger.info("PilotManager refreshed. Now refreshing Pilot.")
            assert pilot is None or isinstance(pilot, rp.Pilot) and pilot.state in rp.FINAL
        if pilot is None:
            logger.info(f"Creating a Pilot for {runtime.session.uid}")
        if isinstance(pilot, rp.Pilot):
            if pilot.state in rp.FINAL:
                logger.info(f"Old Pilot is {pilot.state}")
            else:
                logger.warning(f"Canceling old pilot {pilot.uid}, which should have already been canceled.")
                pilot.cancel()
        pilot = _new_pilot(
            session=runtime.session,
            pilot_manager=runtime.pilot_manager(),
            pilot_description=pilot_description,
            venv=venv,
        )
        if pilot is None:
            raise RuntimeError("Could not get a Pilot.")
        logger.info(f"New Pilot is {pilot.uid}")
        runtime.pilot(pilot)
    else:
        assert pilot is runtime.pilot()


def _check_task_manager(runtime: Runtime, pilot_description: rp.PilotDescription, venv):
    task_manager = runtime.task_manager()
    original_pilot = runtime.pilot()
    _check_pilot(runtime=runtime, pilot_description=pilot_description, venv=venv)
    pilot = runtime.pilot()
    if task_manager is None or pilot is not original_pilot:
        if pilot is not original_pilot and original_pilot is not None:
            logger.info("Pilot has changed. Creating and binding a new TaskManager.")
        if task_manager is not None:
            assert isinstance(task_manager, rp.TaskManager)
            logger.info("Closing old TaskManager.")
            task_manager.close()
        logger.info(f"Creating new TaskManager for {runtime.session.uid}")
        task_manager = _new_taskmanager(session=runtime.session, pilot=pilot)

    if runtime.task_manager() is not task_manager:
        runtime.task_manager(task_manager)


@pytest.fixture(scope="function")
def rp_pilot_manager(rp_runtime: Runtime):
    _check_pilot_manager(runtime=rp_runtime)
    yield rp_runtime.pilot_manager()


@pytest.fixture(scope="function")
def rp_pilot(rp_runtime: Runtime, rp_pilot_manager: rp.PilotManager, pilot_description: rp.PilotDescription, rp_venv):
    _check_pilot(runtime=rp_runtime, pilot_description=pilot_description, venv=rp_venv)
    pilot = rp_runtime.pilot()
    assert pilot is not None
    assert pilot.state not in rp.FINAL
    yield pilot


@pytest.fixture(scope="function")
def rp_task_manager(rp_runtime: Runtime, pilot_description, rp_venv):
    _check_task_manager(runtime=rp_runtime, pilot_description=pilot_description, venv=rp_venv)
    task_manager: rp.TaskManager = rp_runtime.task_manager()
    yield task_manager


@pytest.fixture(scope="session")
def sdist():
    """Build and provide an sdist of the scalems package in its current state."""
    import build

    src_dir = pathlib.Path(__file__).parent.parent
    assert os.path.exists(src_dir / "tests" / "conftest.py")
    with tempfile.TemporaryDirectory() as dir:
        dist = build.ProjectBuilder(str(src_dir)).build(distribution="sdist", output_directory=dir)
        yield dist
