from __future__ import annotations

__all__ = (
    "runtime_session",
    "RuntimeSession",
)

import asyncio
import logging
import threading
import typing
import uuid
import warnings
import weakref

import typing_extensions

from scalems.exceptions import APIError
from scalems.exceptions import ProtocolError
from scalems.radical.runtime_configuration import RuntimeConfiguration

from radical import pilot as rp

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


class RuntimeSession:
    """Container for scalems.radical runtime state data.

    Use a creation function to provide RuntimeSession with an asyncio event
    loop. Interact with the RuntimeSession in the main thread whenever
    possible. Let the RuntimeSession dispatch slow rp UI calls to other
    threads as needed and appropriate.

    Note:
        There is very little automated error recovery. For examples of expansive
        checking and re-launching of runtime resources, refer to the fixtures
        in conftest.py at or before revision 41b965a27c5af9abc115677b738085c35766b5b6.
    """

    resources: typing.Optional[asyncio.Task[dict]] = None
    """The active Pilot resources, if any.

    The runtime_startup routine schedules a Task to get a copy of
    the Pilot.resource_details['rm_info'] dictionary, once the Pilot
    reaches state PMGR_ACTIVE.
    """

    _configuration: RuntimeConfiguration
    _loop: asyncio.AbstractEventLoop
    _pilot_manager: typing.Optional[rp.PilotManager] = None
    _pilot: typing.Optional[rp.Pilot] = None
    _session: rp.Session
    _task_manager: typing.Optional[rp.TaskManager] = None

    def __init__(self, session: rp.Session, *, loop: asyncio.AbstractEventLoop, configuration: RuntimeConfiguration):
        if not isinstance(session, rp.Session) or session.closed:
            raise ValueError("*session* must be an active RADICAL Pilot Session.")
        self._session = session
        # TODO: Call session.close in a ThreadPoolExecutor that we use for RP UI calls.
        self._session_finalizer = weakref.finalize(self, session.close)
        if loop.is_closed():
            raise ValueError("*loop* must be an active event loop.")
        # Note: loop.is_running() may not yet return True if no coroutines have been awaited.
        self._loop = loop
        self._configuration = configuration

    def __repr__(self):
        if session := self._session:
            session = session.uid
        if pilot := self._pilot:
            pilot = pilot.uid
        representation = f'<RuntimeSession "{session}" pilot:"{pilot}">'
        return representation

    async def wait_closed(self):
        """Wait for a closing session to be closed.

        Use with `close()` to allow the asyncio event loop to resolve outstanding tasks.

        TODO: Do we need this?
        This method may not be necessary. Or it may be more necessary in the future.
        """
        while not self.resources.done():
            timer = asyncio.create_task(asyncio.sleep(10.0), name="Session closing timer")
            done, pending = asyncio.wait((self.resources, timer), return_when=asyncio.FIRST_EXCEPTION)
            if self.resources in done:
                timer.cancel()
            else:
                logger.info("Waiting for session to close.")

    def close(self):
        """Direct the runtime to shut down and release resources.

        Warning:
            This function may return before resources have been finalized.
            Follow a call to `close()` with `wait_closed()` to give the event
            loop a chance to cycle.
        """

        # De-initialize state: reset data members to class defaults.

        if self.resources is not None and not self.resources.done():
            if threading.main_thread() == threading.current_thread():
                self.resources.cancel()
            else:
                self._loop.call_soon_threadsafe(self.resources.cancel)

        if self._pilot is not None:
            del self._pilot
        if self._task_manager is not None:
            del self._task_manager
        if self._pilot_manager is not None:
            del self._pilot_manager

        # Note: there are no documented exceptions or errors to check for,
        # programmatically. Some issues encountered during shutdown will be
        # reported through the reporter or logger of the
        # radical.pilot.utils.component.Component base.
        # The RP convention seems to be to use the component uid as the name
        # of the underlying logging.Logger node, so we could presumably attach
        # a log handler to the logger for a component of interest.
        logger.debug(f"Closing Session {self.session.uid}.")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")
            self.session.close(download=True)

        self._session_finalizer()
        del self._session_finalizer

    @property
    def session(self) -> rp.Session:
        """The current radical.pilot.Session (may already be closed)."""
        return self._session

    def _new_pilotmanager(self):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

            return rp.PilotManager(session=self.session)

    @typing.overload
    def pilot_manager(self) -> typing.Union[rp.PilotManager, None]:
        ...

    @typing.overload
    def pilot_manager(self, pilot_manager: str) -> rp.PilotManager:
        ...

    @typing.overload
    def pilot_manager(self, pilot_manager: rp.PilotManager) -> rp.PilotManager:
        ...

    def pilot_manager(self, pilot_manager=None) -> typing.Union[rp.PilotManager, None]:
        """Get (optionally set) the current PilotManager.

        Args:
            pilot_manager (optional, radical.pilot.PilotManager, str):
                Set to RP PilotManager instance or identifier, if provided.

        Returns:
            radical.pilot.PilotManager: instance, if set, else ``None``.

        Raises:
            ValueError: for invalid identifier.
            APIError: for invalid RP Session configuration.
        """
        if pilot_manager is None:
            # Caller should destroy and recreate Pilot if this call has to replace PilotManager.
            session = self.session
            if session.closed:
                # Once rp.Session is closed, require a new RuntimeSession.
                raise ProtocolError(f"RP Session {self.session.uid} is closed. Get a new RuntimeSession instance.")
            if self._pilot_manager is not None:
                return self._pilot_manager
            # Is there a way to check whether the PilotManager is healthy?
            logger.info(f"Creating a new PilotManager for {self.session.uid}")
            manager = self._new_pilotmanager()
            logger.info(f"New PilotManager is {manager.uid}")
            return self.pilot_manager(manager)
        elif isinstance(pilot_manager, rp.PilotManager):
            if self._pilot_manager is not None and pilot_manager != self._pilot_manager:
                raise APIError(f"PilotManager {self._pilot_manager.uid} already assigned.")
            if not pilot_manager.session.uid == self.session.uid:
                raise APIError("Cannot accept a PilotManager from a different Session.")
            self._pilot_manager = pilot_manager
            return self._pilot_manager
        else:
            uid = pilot_manager
            try:
                pmgr = self.session.get_pilot_managers(pmgr_uids=uid)
                assert isinstance(pmgr, rp.PilotManager)
            except (AssertionError, KeyError) as e:
                raise ValueError(f"{uid} does not describe a valid PilotManager") from e
            except Exception as e:
                logger.exception("Unhandled RADICAL Pilot exception.", exc_info=e)
                raise ValueError(f"{uid} does not describe a valid PilotManager") from e
            else:
                return self.pilot_manager(pmgr)

    @typing.overload
    def task_manager(self) -> typing.Union[rp.TaskManager, None]:
        ...

    @typing.overload
    def task_manager(self, task_manager: str) -> rp.TaskManager:
        ...

    @typing.overload
    def task_manager(self, task_manager: rp.TaskManager) -> rp.TaskManager:
        ...

    def task_manager(self, task_manager=None) -> typing.Union[rp.TaskManager, None]:
        """Get (optionally set) the current TaskManager.

        Args:
            task_manager (optional, radical.pilot.TaskManager, str):
                Set to RP TaskManager instance or identifier, if provided.

        Returns:
            radical.pilot.TaskManager: instance, if set, else ``None``.

        Raises:
            ValueError: for invalid identifier.
            APIError: for invalid RP Session configuration.
        """
        if task_manager is None:
            return self._task_manager
        elif isinstance(task_manager, rp.TaskManager):
            if not task_manager.session.uid == self.session.uid:
                raise APIError("Cannot accept a TaskManager from a different Session.")
            self._task_manager = task_manager
            return task_manager
        else:
            uid = task_manager
            try:
                tmgr = self.session.get_task_managers(tmgr_uids=uid)
                assert isinstance(tmgr, rp.TaskManager)
            except (AssertionError, KeyError) as e:
                raise ValueError(f"{uid} does not describe a valid TaskManager") from e
            except Exception as e:
                logger.exception("Unhandled RADICAL Pilot exception.", exc_info=e)
                raise ValueError(f"{uid} does not describe a valid TaskManager") from e
            else:
                return self.task_manager(tmgr)

    @staticmethod
    def _new_pilot(
        *,
        session: rp.Session,
        pilot_manager: rp.PilotManager,
        pilot_description: rp.PilotDescription,
        task_manager: rp.TaskManager,
    ):
        logger.debug(
            "Using resource config: {}".format(str(session.get_resource_config(pilot_description.resource).as_dict()))
        )
        logger.debug("Using PilotDescription: {}".format(str(pilot_description.as_dict())))
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

            pilot = pilot_manager.submit_pilots([rp.PilotDescription(pilot_description)])[0]
            task_manager.add_pilots(pilot)
        return pilot

    def pilot(self) -> rp.Pilot:
        """Get active Pilot.

        Allows lazy initialization of the Pilot resource.

        Returns:
            radical.pilot.Pilot: The current Pilot instance, if available and valid,
                or a new Pilot instance in the configured PilotManager.

        Raises:
            APIError: for invalid RP Session configuration.
        """
        pilot_manager = self.pilot_manager()
        if not pilot_manager:
            raise APIError("Cannot get/set Pilot before setting PilotManager.")

        pilot = self._pilot

        if pilot is None or pilot.state in rp.FINAL:
            if pilot is None:
                logger.info(f"Creating a Pilot for {self.session.uid}")
            else:
                assert isinstance(pilot, rp.Pilot)
                logger.info(f"Old Pilot {pilot.uid} in state {pilot.state}")
            pilot_description = describe_pilot(self._configuration)

            logger.debug("Requesting Pilot: {}".format(repr(pilot_description.as_dict())))
            task_manager = self.task_manager()
            if not task_manager:
                raise APIError("Cannot get/set Pilot before setting TaskManager.")

            pilot = self._new_pilot(
                session=self.session,
                pilot_manager=pilot_manager,
                pilot_description=pilot_description,
                task_manager=task_manager,
            )
            logger.debug(f"Got Pilot {pilot.uid}: {pilot.as_dict()}")

            # Note: This could take hours or days depending on the queuing system.
            # Can we report some more useful information, like job ID?
            # self.resources = self._loop.create_task(pilot_resources(pilot), name="Pilot resources")
            self.resources = asyncio.create_task(get_pilot_resources(pilot), name="Pilot resources")

            self._pilot = pilot
        # Do some checking.
        session = pilot.session
        assert isinstance(session, rp.Session)
        if session.uid != self.session.uid:
            raise APIError("Cannot accept a Pilot from a different Session.")
        if pilot.pmgr.uid != pilot_manager.uid:
            raise APIError("Pilot must be associated with a PilotManager already configured.")

        # TODO: If new, the Pilot referenced will still be starting up. It seems like we
        #  don't know when or if the Pilot will ever actually start. But maybe we should use
        #  a Future to allow for synchronization or error detection.
        return pilot


def describe_pilot(configuration: RuntimeConfiguration):
    pilot_description_dict = configuration.rp_resource_params["PilotDescription"].copy()
    # Get a unique identifier.
    pilot_description_dict["uid"] = f"pilot.{str(uuid.uuid4())}"
    pilot_description_dict["resource"] = configuration.execution_target
    assert pilot_description_dict["exit_on_error"] is False
    # if pilot_description_dict.get("exit_on_error", True):
    #     warnings.warn("Failing to set PilotDescription.exit_on_error to False may prevent clean shut down.")
    pilot_description = rp.PilotDescription(pilot_description_dict)
    return pilot_description


def _rp_session(*args, **kwargs) -> rp.Session:
    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        # This would be a good time to `await`, if an event-loop friendly
        # Session creation function becomes available.
        session = rp.Session(*args, **kwargs)
        logger.info(f"Created {session.uid}")
    return session


async def runtime_session(*, configuration: RuntimeConfiguration, loop=None) -> RuntimeSession:
    """Start a new RADICAL Pilot Session.

    Returns:
        RuntimeSession instance.

    """
    if loop is None:
        loop = asyncio.get_running_loop()
    _task = asyncio.create_task(asyncio.to_thread(_rp_session), name="create-Session")
    session: rp.Session = await _task
    runtime = RuntimeSession(session=session, loop=loop, configuration=configuration)

    # At some point soon, we need to track Session ID for the workflow metadata.
    session_id = runtime.session.uid
    # Do we want to log this somewhere?
    # session_config = copy.deepcopy(self.session.cfg.as_dict())
    logger.debug("Acquired RP Session {}".format(session_id))

    logger.debug("Launching PilotManager.")
    pilot_manager = await asyncio.create_task(
        asyncio.to_thread(rp.PilotManager, session=runtime.session),
        name="get-PilotManager",
    )
    pilot_manager = runtime.pilot_manager(pilot_manager)
    logger.debug("Got PilotManager {}.".format(pilot_manager.uid))

    logger.debug("Launching TaskManager.")
    task_manager = await asyncio.create_task(
        asyncio.to_thread(rp.TaskManager, session=runtime.session),
        name="get-TaskManager",
    )
    task_manager = runtime.task_manager(task_manager)
    logger.debug(("Got TaskManager {}".format(task_manager.uid)))

    #
    # Get a Pilot
    #
    # We can launch an initial Pilot, but we may have to run further Pilots
    # during self._queue_runner_task (or while servicing scalems.wait() within the
    # with block) to handle dynamic work load requirements.
    # Optionally, we could refrain from launching the pilot here, at all,
    # but it seems like a good chance to start bootstrapping the agent environment.
    #
    # How and when should we update the pilot description?

    pilot = runtime.pilot()
    logger.debug("Added Pilot {} to task manager {}.".format(pilot.uid, runtime.task_manager().uid))

    return runtime


class RmInfo(typing_extensions.TypedDict):
    # Refer to https://github.com/radical-cybertools/radical.pilot/issues/2973
    # for evolution of a more stable interface.
    # See issue #367
    requested_cores: int
    requested_gpus: int


async def get_pilot_resources(pilot: rp.Pilot) -> RmInfo:
    def log_pilot_state(fut: asyncio.Task[str]):
        if not fut.cancelled():
            if e := fut.exception():
                logger.exception("Exception while watching for Pilot to become active.", exc_info=e)
        logger.info(f"Pilot {pilot.uid} in state {pilot.state}.")

    logger.info("Waiting for an active Pilot.")
    # Wait for Pilot to be in state PMGR_ACTIVE. (There is no reasonable
    # choice of a timeout because we are waiting for the HPC queuing system.)
    # Then, query Pilot.resource_details['rm_info']['requested_cores'] and 'requested_gpus'.
    pilot_state = asyncio.create_task(
        asyncio.to_thread(pilot.wait, state=rp.PMGR_ACTIVE, timeout=None), name="pilot_state_waiter"
    )

    pilot_state.add_done_callback(log_pilot_state)
    await pilot_state
    rm_info: RmInfo = pilot.resource_details.get("rm_info")
    logger.debug(f"Pilot {pilot.uid} resources: {str(rm_info)}")
    if rm_info is not None:
        assert "requested_cores" in rm_info and isinstance(rm_info["requested_cores"], int)
        assert "requested_gpus" in rm_info and isinstance(rm_info["requested_gpus"], int)
        return rm_info.copy()
