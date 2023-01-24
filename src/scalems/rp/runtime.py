"""Manage the RADICAL Pilot start-up and shut-down.

The provided Runtime class encapsulates stateful resources that, once acquired,
should be shut down explicitly. Runtime instances may be used as context managers to
ensure the proper protocol is followed, or the caller can take responsibility for
calling Runtime.close() to shut down.

Note: Consider whether Runtime context manager is reentrant or multi-use.

The Runtime state encapsulates several nested states. A Session, TaskManager,
and PilotManager must be created for the Runtime to be usable. Additionally, Pilots and
scheduler tasks may be added or removed during the Runtime lifetime. To better support
the alternate scenarios when a Runtime instance may be provided to a scalems.rp
component in an arbitrary state, consider making the ``with`` block scoped, such that
it only returns the Runtime instance to its initial state when exiting, rather than
shutting down completely.
See also https://docs.python.org/3/library/contextlib.html#contextlib.ExitStack

Deferred:
    Runtime can avoid providing direct access to RP interface, and instead run an
    entire RP Session state machine in a thread (separate from the asyncio event loop
    thread), relaying RP scripting commands through queues, in order to completely
    prevent misuse and to insulate the asyncio event loop from blocking RP commands.
    We need to get a better sense of the RP flow combinatorics before we can reasonably
    pursue this.

See Also:
    https://github.com/SCALE-MS/scale-ms/issues/55

..
    .. uml::

        title scalems on radical.pilot run time

        box "SCALE-MS framework" #honeydew
        participant WorkflowManager as client_workflowmanager
        participant RuntimeManager
        participant "runner task" as scalems.execution
        end box
        box "SCALE-MS RP adapter" #linen
        participant Runtime as client_runtime
        participant Executor as client_executor
        end box

        autoactivate on

        client_workflowmanager -> client_executor: async with executor
        activate client_workflowmanager
        client_executor -> RuntimeManager: ~__aenter__()
        RuntimeManager -> client_executor: runtime_configuration()
        RuntimeManager -> client_executor: runtime_startup()

        client_executor -> : rp.Session()
        return
        client_executor -> client_runtime **: Session
        activate client_runtime
        client_executor -> : rp.PilotManager()
        return
        client_executor -> client_runtime: pilot_manager()
        return
        client_executor -> : rp.TaskManager()
        return
        client_executor -> client_runtime: task_manager()
        return
        client_executor -> : pilot_manager.submit_pilots()
        return
        client_executor -> client_runtime: pilot()
        note left
        Pilot venv is determined by resource definition (JSON file).
        end note
        return

        group ref [scalems.radical.raptor]
        client_executor -> client_executor: get_scheduler()
        return
        end

        client_executor -> client_runtime: set scheduler
        return

        client_executor -> scalems.execution **: create_task(manage_execution)
        client_executor -> scalems.execution: await runner_started
        RuntimeManager <-- client_executor: set runner_task
        deactivate client_executor
        deactivate RuntimeManager

        client_workflowmanager -> client_workflowmanager #gray: async with dispatcher

        ...Raptor workload handling...

        return leave dispatcher context

        == Shut down runtime ==

        client_executor -> RuntimeManager: ~__aexit__()
        RuntimeManager -> RuntimeManager: await runner_task
        RuntimeManager <-- scalems.execution
        deactivate RuntimeManager
        RuntimeManager -> client_executor: runtime_shutdown()

        client_runtime <- client_executor
        return session
        client_runtime <- client_executor
        return scheduler
        group Cancel Master task [if scheduler is not None]
        client_runtime <- client_executor
        return task_manager
        client_executor -> : task_manager.cancel_tasks()
        return
        client_runtime <- client_executor
        return scheduler
        client_executor -> : runtime.scheduler.wait()
        return
        end
        client_executor -> : session.close()
        return

        client_executor -> client_runtime !!
        deactivate client_executor
        deactivate RuntimeManager

        client_workflowmanager <-- client_executor: leave executor context
        deactivate client_workflowmanager

"""

from __future__ import annotations

__all__ = (
    "configuration",
    "executor_factory",
    "parser",
    "Configuration",
    "Runtime",
)

import argparse
import asyncio
import collections.abc
import contextlib
import contextvars
import dataclasses
import functools
import logging
import os
import pathlib
import threading
import typing
import uuid
import warnings
import weakref

from radical import pilot as rp

import scalems.compat
import scalems.exceptions
import scalems.execution
import scalems.invocation
import scalems.subprocess
import scalems.workflow
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import InternalError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.exceptions import ScaleMSError
from ..store import FileStore
from ..execution import AbstractWorkflowUpdater
from ..execution import RuntimeManager
from ..identifiers import TypeIdentifier

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

# TODO: Consider scoping for these context variables.
# Need to review PEP-567 and PEP-568 to consider where and how to scope the Context
# with respect to the dispatching scope.
_configuration = contextvars.ContextVar("_configuration")

try:
    cache = functools.cache
except AttributeError:
    # Note: functools.cache does not appear until Python 3.9
    cache = functools.lru_cache(maxsize=None)


def _parse_option(arg: str) -> tuple:
    if not isinstance(arg, str):
        raise InternalError("Bug: This function should only be called with a str.")
    if arg.count("=") != 1:
        raise argparse.ArgumentTypeError('Expected a key/value pair delimited by "=".')
    return tuple(arg.split("="))


@cache
def parser(add_help=False):
    """Get the module-specific argument parser.

    Provides a base argument parser for scripts using the scalems.rp backend.

    By default, the returned ArgumentParser is created with ``add_help=False``
    to avoid conflicts when used as a *parent* for a parser more local to the caller.
    If *add_help* is provided, it is passed along to the ArgumentParser created
    in this function.

    See Also:
         https://docs.python.org/3/library/argparse.html#parents
    """
    _parser = argparse.ArgumentParser(add_help=add_help, parents=[scalems.invocation.base_parser()])

    # We could consider inferring a default venv from the VIRTUAL_ENV environment
    # variable,
    # but we currently have very poor error handling regarding venvs. For now, this needs
    # to be explicit.
    # Ref https://github.com/SCALE-MS/scale-ms/issues/89
    # See also https://github.com/SCALE-MS/scale-ms/issues/90
    # TODO: Set module variables rather than carry around an args namespace?
    _parser.add_argument(
        "--venv",
        metavar="PATH",
        type=str,
        required=True,
        help="Path to the (pre-configured) Python virtual "
        "environment with which RP tasks should be executed. "
        "(Required. See also https://github.com/SCALE-MS/scale-ms/issues/90)",
    )

    _parser.add_argument(
        "--resource",
        type=str,
        required=True,
        help="Specify a `RP resource` for the radical.pilot.PilotDescription. (Required)",
    )

    _parser.add_argument(
        "--access",
        type=str,
        help="Explicitly specify the access_schema to use from the RADICAL resource.",
    )

    _parser.add_argument(
        "--pilot-option",
        action="append",
        type=_parse_option,
        metavar="<key>=<value>",
        help="Add a key value pair to the `radical.pilot.PilotDescription`.",
    )
    return _parser


@dataclasses.dataclass(frozen=True)
class Configuration:
    """Module configuration information.

    See also:
        * :py:func:`scalems.rp.configuration`
        * :py:data:`scalems.rp.runtime.parser`
        * :py:class:`scalems.rp.runtime.Runtime`

    .. todo:: Consider merging with module Runtime state container.

    """

    # Note that the use cases for this dataclass interact with module ContextVars,
    # pending refinement.
    datastore: FileStore = None
    # TODO: Check that the resource is defined.
    execution_target: str = "local.localhost"
    rp_resource_params: dict = dataclasses.field(default_factory=dict)
    target_venv: str = None


class Runtime:
    """Container for scalems.rp runtime state data.

    Note:
        This class is almost exclusively a container. Lifetime management is assumed
        to be handled externally.

    .. todo:: Consider either merging with `scalems.rp.runtime.Configuration` or
        explicitly encapsulating the responsibilities of `RPDispatchingExecutor.runtime_startup()`
        and `RPDispatchingExecutor.runtime_shutdown()`.

    See Also:
        :py:attr:`scalems.rp.runtime.RPDispatchingExecutor.runtime`

    """

    _session: rp.Session

    _pilot_manager: typing.Optional[rp.PilotManager] = None
    _pilot: typing.Optional[rp.Pilot] = None
    _task_manager: typing.Optional[rp.TaskManager] = None

    def __init__(self, session: rp.Session):
        if not isinstance(session, rp.Session) or session.closed:
            raise ValueError("*session* must be an active RADICAL Pilot Session.")
        self._session = session

    def reset(self, session: rp.Session):
        """Reset the runtime state.

        Close any existing resources and revert to a new Runtime state containing only
        the provided *session*.
        """
        if not isinstance(session, rp.Session) or session.closed:
            raise ValueError("*session* must be an active RADICAL Pilot Session.")
        self._session.close()
        # Warning: This is not quite right.
        # The attribute values are deferred to the class dict from initialization. The
        # following lines actually leave the instance members in place with None values
        # rather than removing them, but the logic of checking for and removing the
        # instance values seems a little harder to read.
        self._pilot = None
        self._task_manager = None
        self._pilot_manager = None
        self._session = session

    @property
    def session(self) -> rp.Session:
        """The current radical.pilot.Session (may already be closed)."""
        return self._session

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
            return self._pilot_manager
        elif isinstance(pilot_manager, rp.PilotManager):
            if not pilot_manager.session.uid == self.session.uid:
                raise APIError("Cannot accept a PilotManager from a different Session.")
            self._pilot_manager = pilot_manager
            return pilot_manager
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

    @typing.overload
    def pilot(self) -> typing.Union[rp.Pilot, None]:
        ...

    @typing.overload
    def pilot(self, pilot: str) -> rp.Pilot:
        ...

    @typing.overload
    def pilot(self, pilot: rp.Pilot) -> rp.Pilot:
        ...

    def pilot(self, pilot=None) -> typing.Union[rp.Pilot, None]:
        """Get (optionally set) the current Pilot.

        Args:
            pilot (radical.pilot.Pilot, str, None): Set to RP Pilot instance or identifier, if provided.

        Returns:
            radical.pilot.Pilot: instance, if set, else ``None``

        Raises:
            ValueError: for invalid identifier.
            APIError: for invalid RP Session configuration.
        """
        if pilot is None:
            return self._pilot

        pmgr = self.pilot_manager()
        if not pmgr:
            raise APIError("Cannot set Pilot before setting PilotManager.")

        if isinstance(pilot, rp.Pilot):
            session = pilot.session
            if not isinstance(session, rp.Session):
                raise APIError(f"Pilot {repr(pilot)} does not have a valid Session.")
            if session.uid != self.session.uid:
                raise APIError("Cannot accept a Pilot from a different Session.")
            if pilot.pmgr.uid != pmgr.uid:
                raise APIError("Pilot must be associated with a PilotManager already configured.")
            self._pilot = pilot
            return pilot
        else:
            uid = pilot
            try:
                pilot = pmgr.get_pilots(uids=uid)
                assert isinstance(pilot, rp.Pilot)
            except (AssertionError, KeyError, ValueError) as e:
                raise ValueError(f"{uid} does not describe a valid Pilot") from e
            except Exception as e:
                # TODO: Track down the expected rp exception.
                logger.exception("Unhandled RADICAL Pilot exception.", exc_info=e)
                raise ValueError(f"{uid} does not describe a valid Pilot") from e
            else:
                return self.pilot(pilot)


# functools can't cache this function while Configuration is unhashable (due to
# unhashable dict member).
# @cache
def get_pre_exec(conf: Configuration) -> tuple:
    """Get the sequence of pre_exec commands for tasks on the currently configured execution target.

    Warning:
        Use cases may require a `list` object. Caller is responsible for converting
        the returned tuple if appropriate.

    """
    if conf.target_venv is None or len(conf.target_venv) == 0:
        raise ValueError("Currently, tasks cannot be dispatched without a target venv.")

    activate_venv = ". " + str(os.path.join(conf.target_venv, "bin", "activate"))
    # Note: RP may specifically expect a `list` and not a `tuple`.
    sequence = (activate_venv,)
    return sequence


@functools.singledispatch
def _set_configuration(*args, **kwargs) -> Configuration:
    """Initialize or retrieve the module configuration.

    This module and the RADICAL infrastructure have various stateful aspects
    that require clearly-scoped module-level configuration. Module configuration
    should be initialized exactly once per Python process.

    Recommended usage is to derive an ArgumentParser from the *parser()* module
    function and use the resulting namespace to initialize the module configuration
    using this function.

    Note this is a dispatch function. "Overloads" are defined in separate decorated
    functions for calls that provide a `Configuration` or `argparse.Namespace`
    object as the first (and only) positional argument.
    """
    assert len(args) != 0 or len(kwargs) != 0
    # Caller has provided arguments.
    # Not thread-safe
    if _configuration.get(None):
        raise APIError(f"configuration() cannot accept arguments when {__name__} is already configured.")
    c = Configuration(*args, **kwargs)
    _configuration.set(c)
    return _configuration.get()


@_set_configuration.register
def _(config: Configuration) -> Configuration:
    # Not thread-safe
    if _configuration.get(None):
        raise APIError(f"configuration() cannot accept arguments when {__name__} is already configured.")
    _configuration.set(config)
    return _configuration.get()


@_set_configuration.register
def _(namespace: argparse.Namespace) -> Configuration:
    rp_resource_params = {
        "PilotDescription": {
            "access_schema": namespace.access,
            "exit_on_error": False,
        }
    }
    if namespace.pilot_option is not None and len(namespace.pilot_option) > 0:
        user_options = _PilotDescriptionProxy.normalize_values(namespace.pilot_option)
        rp_resource_params["PilotDescription"].update(user_options)
        logger.debug(f'Pilot options: {repr(rp_resource_params["PilotDescription"])}')

    config = Configuration(
        execution_target=namespace.resource,
        target_venv=namespace.venv,
        rp_resource_params=rp_resource_params,
    )
    return _set_configuration(config)


async def new_session():
    """Start a new RADICAL Pilot Session.

    Returns:
        Runtime instance.

    """
    to_thread = scalems.compat.get_to_thread()

    # Note that we cannot resolve the full _resource config until we have a Session
    # object.
    # We cannot get the default session config until after creating the Session,
    # so we don't have a template for allowed, required, or default values.
    # Question: does the provided *cfg* need to be complete? Or will it be merged
    # with default values from some internal definition, such as by dict.update()?
    # I don't remember what the use cases are for overriding the default session
    # config.
    session_config = None

    # Note: We may soon want Session ID to be deterministic (or to be re-used?).
    session_id = None

    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        # This would be a good time to `await`, if an event-loop friendly
        # Session creation function becomes available.
        session_args = dict(uid=session_id, cfg=session_config)
        _task = asyncio.create_task(to_thread(rp.Session, (), **session_args), name="create-Session")
        session = await _task
        runtime = Runtime(session=session)
    return runtime


@functools.singledispatch
def normalize(hint: object, value):
    """Dispatching value normalizer.

    Normalize value according to hint.

    Raises:
        MissingImplementationError: if key could not be dispatched for normalization.
        TypeError: if value could not be normalized according to hint.

    """
    raise MissingImplementationError(f"No dispatcher for {repr(value)} -> {repr(hint)}.")


@normalize.register
def _(hint: type, value):
    return hint(value)


@normalize.register
def _(hint: list, value):
    if len(hint) != 1:
        raise InternalError(f"Expected a list of one type element. Got {repr(hint)}.")
    if isinstance(value, (str, bytes)) or not isinstance(value, collections.abc.Iterable):
        raise TypeError(f"Expected a list-like value. Got {repr(value)}.")
    return [normalize(hint[0], element) for element in value]


@normalize.register
def _(hint: dict, value):
    try:
        for key in value.keys():
            if key not in hint:
                raise MissingImplementationError(f"{key} is not a valid field.")
        items: tuple = value.items()
    except AttributeError:
        raise TypeError(f"Expected a dict-like value. Got {repr(value)}.")
    return {key: normalize(hint[key], val) for key, val in items}


class _PilotDescriptionProxy(rp.PilotDescription):
    """Use PilotDescription details to normalize the value types of description fields."""

    assert hasattr(rp.PilotDescription, "_schema")
    assert isinstance(rp.PilotDescription._schema, dict)
    assert all(
        map(
            lambda v: isinstance(v, (type, list, dict, type(None))),
            rp.PilotDescription._schema.values(),
        )
    )

    @classmethod
    def normalize_values(cls, desc: typing.Sequence[tuple]):
        """Generate normalized key-value tuples.

        For values that are not already of the appropriate type, cast according to
        PilotDescription._schema.

        Args:
            desc: sequence of key-value tuples for PilotDescription fields.

        Raises:
            MissingImplementationError: if key could not be dispatched for normalization.
            TypeError: if value could not be normalized according to hint.

        """
        for key, value in desc:
            try:
                hint = cls._schema[key]
            except KeyError:
                raise MissingImplementationError(f"{key} is not a valid PilotDescription field.")
            if not isinstance(hint, type):
                # This may be overly aggressive, but at the moment we are only normalizing values from
                # the command line parser, and we don't have a good way to pre-parse list or dict values.
                raise MissingImplementationError(f"No handler for {key} field of type {repr(hint)}.")

            if isinstance(None, hint) or isinstance(value, hint):
                yield key, value
            else:
                yield key, normalize(hint, value)


async def add_pilot(runtime: Runtime, **kwargs):
    """Get a new rp.Pilot and add it to the Runtime instance."""
    if kwargs:
        message = (
            f"scalems.rp.runtime.add_pilot() version {scalems.__version__} "
            + "does not support keyword arguments "
            + ", ".join(kwargs.keys())
        )
        raise MissingImplementationError(message)
    pilot_manager = runtime.pilot_manager()
    task_manager = runtime.task_manager()
    if not pilot_manager or not task_manager:
        raise ProtocolError(f"Runtime {runtime} is not ready to use.")

    # Note: Consider fetching through the Runtime instance.
    config: Configuration = configuration()

    to_thread = scalems.compat.get_to_thread()

    # Warning: The Pilot ID needs to be unique within the Session.
    pilot_description = {"uid": f"pilot.{str(uuid.uuid4())}"}
    pilot_description.update(config.rp_resource_params.get("PilotDescription", {}))
    pilot_description.update({"resource": config.execution_target})

    # TODO: Pilot venv (#90, #94).
    # Currently, Pilot venv must be specified in the JSON file for resource
    # definitions.
    pilot_description = rp.PilotDescription(pilot_description)
    logger.debug("Submitting PilotDescription {}".format(repr(pilot_description.as_dict())))
    pilot: rp.Pilot = await asyncio.create_task(
        to_thread(pilot_manager.submit_pilots, pilot_description), name="submit_pilots"
    )
    logger.debug(f"Got Pilot {pilot.uid}: {pilot.as_dict()}")
    runtime.pilot(pilot)

    await asyncio.create_task(to_thread(task_manager.add_pilots, pilot), name="add_pilots")

    return pilot


class RPDispatchingExecutor(RuntimeManager):
    """Client side manager for work dispatched through RADICAL Pilot.

    Configuration points:

    * resource config
    * pilot config
    * session config?

    Extends :py:class:`scalems.execution.RuntimeManager`
    """

    runtime: "scalems.rp.runtime.Runtime"
    """See `scalems.execution.RuntimeManager.runtime`"""

    def __init__(
        self,
        *,
        editor_factory: typing.Callable[[], typing.Callable] = None,
        datastore: FileStore = None,
        loop: asyncio.AbstractEventLoop,
        configuration: Configuration,
        dispatcher_lock=None,
    ):
        """Create a client side execution manager.

        Warning:
            The creation method does not fully initialize the instance.

            Initialization and de-initialization occurs through
            the Python (async) context manager protocol.

        """
        if "RADICAL_PILOT_DBURL" not in os.environ:
            raise DispatchError("RADICAL Pilot environment is not available.")

        if not isinstance(configuration.target_venv, str) or len(configuration.target_venv) == 0:
            raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

        super().__init__(
            editor_factory=editor_factory,
            datastore=datastore,
            loop=loop,
            configuration=configuration,
            dispatcher_lock=dispatcher_lock,
        )

    @contextlib.contextmanager
    def runtime_configuration(self):
        """Provide scoped Configuration.

        Merge the runtime manager's configuration with the global configuration,
        update the global configuration, and yield the configuration for a ``with`` block.

        Restores the previous global configuration when exiting the ``with`` block.

        Warning:
            Not thread-safe.

            We do not check for re-entrance, which will cause race conditions w.r.t.
            which Context state is restored! Moreover, the Configuration object is not
            currently hashable and does not have an equality test defined.

            This contextmanager is not async, but it could be (and is) used within
            an asynchronous context manager, so we don't have anything structurally
            prohibiting reentrant calls, even without multithreading.

        TODO:
            Reconsider logic for runtime module scoped Configuration management.

            Instead of managing the current contextvars.Context,
            this should be used to produce an updated contextvars.Context for use by
            a Task or Context.run() scope. Alternatively, we can decouple the runtime configuration
            from the global module configuration, reduce the dynamic utility of
            *<module>.configuration()*, and not use contextvars.

            Design notes:
                Do we want two-way interaction between module
                and instance configuration? Under what circumstances will one or the other
                change during execution? Should we be providing the configuration through
                the current Context, through a Context instance (usable for Context.run() or
                to the task launching command), or simply as a Configuration object?
        """

        # Get default configuration.
        configuration_dict = dataclasses.asdict(configuration())
        # Update with any internal configuration.
        if self._runtime_configuration.target_venv is not None and len(self._runtime_configuration.target_venv) > 0:
            configuration_dict["target_venv"] = self._runtime_configuration.target_venv
        if len(self._runtime_configuration.rp_resource_params) > 0:
            configuration_dict["rp_resource_params"].update(self._runtime_configuration.rp_resource_params)
        if (
            self._runtime_configuration.execution_target is not None
            and len(self._runtime_configuration.execution_target) > 0
        ):
            configuration_dict["execution_target"] = self._runtime_configuration.execution_target
        configuration_dict["datastore"] = self.datastore

        c = Configuration(**configuration_dict)

        token = _configuration.set(c)
        try:
            yield c
        finally:
            _configuration.reset(token)

    async def runtime_startup(self) -> asyncio.Task:
        """Establish the RP Session.

        Acquire a maximally re-usable set of RP resources. The scope established by
        this function is as broad as it can be within the life of the workflow manager.

        Once *instance.runtime_startup()* succeeds, *instance.runtime_shutdown()*
        must be called to clean up resources.
        Use the async context manager behavior of the instance to
        automatically follow this protocol. I.e. instead of calling
        ``instance.runtime_startup(); ...; instance.runtime_shutdown()``,
        use::

            async with instance:
                ...

        Raises:
            DispatchError: if task dispatching could not be set up.
            asyncio.CancelledError: if parent `asyncio.Task` is cancelled while executing.

        """
        _: Configuration = configuration()

        # TODO: Check that we have a FileStore.

        # We try to wrap rp UI calls in separate threads. Note, though, that
        #   * The rp.Session needs to be created in the root thread to be able to correctly
        #     manage signal handlers and subprocesses, and
        #   * We need to be able to schedule RP Task callbacks in the same process as the
        #     asyncio event loop in order to handle Futures for RP tasks.
        # See https://github.com/SCALE-MS/randowtal/issues/2

        # TODO: RP triggers SIGINT in various failure modes.
        #  We should use loop.add_signal_handler() to convert to an exception
        #  that we can raise in an appropriate task. However, we should make sure that we
        #  account for the signal handling that RP expects to be able to do.
        #  See https://github.com/SCALE-MS/randowtal/issues/1
        #  We should also make sure that scalems and RP together handle keyboard interrupts
        #  as a user would expect.
        # Note that PilotDescription can use `'exit_on_error': False` to suppress the SIGINT,
        # but we have not fully explored the consequences of doing so.

        to_thread = scalems.compat.get_to_thread()
        try:
            #
            # Start the Session.
            #

            # Note: the current implementation implies that only one Task for the dispatcher
            # will exist at a time. We are further assuming that there will probably only
            # be one Task per the lifetime of the dispatcher object.
            # We could choose another approach and change our assumptions, if appropriate.
            logger.debug("Entering RP dispatching context. Waiting for rp.Session.")

            _runtime: Runtime = await new_session()
            # At some point soon, we need to track Session ID for the workflow metadata.
            session_id = _runtime.session.uid
            # Do we want to log this somewhere?
            # session_config = copy.deepcopy(self.session.cfg.as_dict())
            logger.debug("RP dispatcher acquired session {}".format(session_id))

            logger.debug("Launching PilotManager.")
            pilot_manager = await asyncio.create_task(
                to_thread(rp.PilotManager, session=_runtime.session),
                name="get-PilotManager",
            )
            logger.debug("Got PilotManager {}.".format(pilot_manager.uid))
            _runtime.pilot_manager(pilot_manager)

            logger.debug("Launching TaskManager.")
            task_manager = await asyncio.create_task(
                to_thread(rp.TaskManager, session=_runtime.session),
                name="get-TaskManager",
            )
            logger.debug(("Got TaskManager {}".format(task_manager.uid)))
            _runtime.task_manager(task_manager)

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

            pilot = await add_pilot(runtime=_runtime)
            logger.debug("Added Pilot {} to task manager {}.".format(pilot.uid, _runtime.task_manager().uid))
        except asyncio.CancelledError as e:
            raise e
        except Exception as e:
            logger.exception("Exception while connecting RADICAL Pilot.", exc_info=e)
            raise DispatchError("Failed to launch SCALE-MS master task.") from e

        self.runtime = _runtime

        if self.runtime is None or self.runtime.session.closed:
            raise ProtocolError("Cannot process queue without a RP Session.")

        # Launch queue processor (proxy executor).
        # TODO: Make runtime_startup optional. Let it return a resource that is
        #  provided to the normalized run_executor(), or maybe use it to configure the
        #  Submitter that will be provided to the run_executor.
        runner_started = asyncio.Event()
        runner_task = asyncio.create_task(scalems.execution.manage_execution(self, processing_state=runner_started))
        await runner_started.wait()
        # TODO: Note the expected scope of the runner_task lifetime with respect to
        #  the global state changes (i.e. ContextVars and locks).
        return runner_task

    @staticmethod
    def runtime_shutdown(runtime: Runtime):
        """Manage tear down of the RADICAL Pilot Session and resources.

        Several aspects of the RP runtime interface use blocking calls.
        This method should be run in a non-root thread (concurrent.futures.Future)
        that the event loop can manage as an asyncio-wrapped task.

        Overrides :py:class:`scalems.execution.RuntimeManager`
        """
        session: rp.Session = getattr(runtime, "session", None)
        if session is None:
            raise scalems.exceptions.APIError(f"No Session in {runtime}.")
        if session.closed:
            logger.error("Runtime Session is already closed?!")
        else:
            # Note: there are no documented exceptions or errors to check for,
            # programmatically. Some issues encountered during shutdown will be
            # reported through the reporter or logger of the
            # radical.pilot.utils.component.Component base.
            # The RP convention seems to be to use the component uid as the name
            # of the underlying logging.Logger node, so we could presumably attach
            # a log handler to the logger for a component of interest.
            logger.debug(f"Closing Session {session.uid}.")
            session.close(download=True)

            if session.closed:
                logger.debug(f"Session {session.uid} closed.")
            else:
                logger.error(f"Session {session.uid} not closed!")
        logger.debug("Runtime shut down.")

    def updater(self) -> "WorkflowUpdater":
        return WorkflowUpdater(executor=self)


def configuration(*args, **kwargs) -> Configuration:
    """Get (and optionally set) the RADICAL Pilot configuration.

    With no arguments, returns the current configuration. If a configuration has
    not yet been set, the command line parser is invoked to try to build a new
    configuration.

    If arguments are provided, try to construct a `scalems.rp.runtime.Configuration`
    and use it to initialize the module.

    It is an error to try to initialize the module more than once.
    """
    # Not thread-safe
    from scalems.rp import parser

    if len(args) > 0:
        _set_configuration(*args, **kwargs)
    elif len(kwargs) > 0:
        _set_configuration(Configuration(**kwargs))
    elif _configuration.get(None) is None:
        # No config is set yet. Generate with module parser.
        namespace, _ = parser.parse_known_args()
        _set_configuration(namespace)
    return _configuration.get()


def executor_factory(manager: scalems.workflow.WorkflowManager, params: Configuration = None):
    if params is not None:
        _set_configuration(params)
    params = configuration()

    executor = RPDispatchingExecutor(
        editor_factory=weakref.WeakMethod(manager.edit_item),
        datastore=manager.datastore(),
        loop=manager.loop(),
        configuration=params,
        dispatcher_lock=manager._dispatcher_lock,
    )
    return executor


class RPResult:
    """Basic result type for RADICAL Pilot tasks.

    Define a return type for Futures or awaitable tasks from
    RADICAL Pilot commands.
    """

    # TODO: Provide support for RP-specific versions of standard SCALEMS result types.


class RPTaskFailure(ScaleMSError):
    """Error in radical.pilot.Task execution.

    Attributes:
        failed_task: A dictionary representation of the failed task.

    TODO: What can/should we capture and report from the failed task?
    """

    failed_task: dict

    def __init__(self, *args, task: rp.Task):
        super().__init__(*args)
        self.failed_task = task.as_dict()


class RPInternalError(ScaleMSError):
    """RADICAL Pilot is misbehaving, probably due to a bug.

    Please report the potential bug.
    """


class RPFinalTaskState:
    # TODO: Provide a bridge between the threading and asyncio Event primitives so that
    #  we can effectively `await` for the events. Presumably this would look like an
    #  asyncio.Future created from a threading.Future of a watcher of the three Events.
    #  Use asyncio.wrap_future() to wrap a threading.Future to an asyncio.Future.
    #  If there is not an adapter, we can loop.run_in_executor() to run the
    #  threading.Event watcher in a separate thread.
    # For future reference, check out the `sched` module.
    def __init__(self):
        self.canceled = threading.Event()
        self.done = threading.Event()
        self.failed = threading.Event()

    def __bool__(self):
        return self.canceled.is_set() or self.done.is_set() or self.failed.is_set()

    def __repr__(self):
        return (
            f"<{self.__class__.__qualname__} "
            f"canceled={self.canceled.is_set()} "
            f"done={self.done.is_set()} "
            f"failed={self.failed.is_set()}>"
        )


def _rp_callback(
    obj: rp.Task,
    state,
    cb_data: weakref.ReferenceType = None,
    final: RPFinalTaskState = None,
):
    """Prototype for RP.Task callback.

    To use, partially bind the *final* parameter (with `functools.partial`) to get a
    callable with the RP.Task callback signature.

    Register with *task* to be called when the rp.Task state changes.
    """
    if final is None:
        raise APIError("This function is strictly for dynamically prepared RP callbacks through " "functools.partial.")
    logger.debug(f"Callback triggered by {repr(obj)} state change to {repr(state)}.")
    try:
        # Note: assertions and exceptions are not useful in RP callbacks.
        if state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED):
            # TODO: Pending https://github.com/radical-cybertools/radical.pilot/issues/2444
            # tmgr: rp.TaskManager = obj.tmgr
            # ref = cb_data()
            # tmgr.unregister_callback(cb=ref, metrics='TASK_STATE',
            #                          uid=obj.uid)
            logger.debug(f"Recording final state {state} for {repr(obj)}")
            if state == rp.states.DONE:
                final.done.set()
            elif state == rp.states.CANCELED:
                final.canceled.set()
            elif state == rp.states.FAILED:
                final.failed.set()
            else:
                logger.error("Bug: logic error in state cases.")
    except Exception as e:
        logger.error(f"Exception encountered during rp.Task callback: {repr(e)}")


async def _rp_task_watcher(task: rp.Task, final: RPFinalTaskState, ready: asyncio.Event) -> rp.Task:  # noqa: C901
    """Manage the relationship between an RP.Task and a scalems Future.

    Cancel the RP.Task if this task or the scalems.Future is canceled.

    Publish the RP.Task result or cancel the scalems.Future if the RP.Task is
    done or canceled.

    Arguments:
        task: RADICAL Pilot Task, submitted by caller.
        final: thread-safe event handler for the RP task call-back to announce it has run.
        ready: output parameter, set when coroutine has run enough to perform its
            responsibilities.

    Returns:
        *task* in its final state.

    An asyncio.Future based on this coroutine has very similar semantics to the
    required *future* argument, but this is subject to change.
    The coroutine is intended to facilitate progress of the task,
    regardless of the rp.Task results. The provided *future* allows the rp.Task
    results to be interpreted and semantically translated. rp.Task failure is
    translated into an exception on *future*. The *future* has a different
    exposure than the coroutine return value, as well: again, the *future* is
    connected to the workflow item and user-facing interface, whereas this
    coroutine is a detail of the task management. Still, these two modes of output
    are subject to revision without notice.

    Caller should await the *ready* event before assuming the watcher task is doing its
    job.
    """

    async def wait_for_final(state: RPFinalTaskState) -> RPFinalTaskState:
        """Function to watch for final event.

        This is a poor design. We should replace with a queuing system for state
        transitions as described in scalems.workflow.
        """
        while not state:
            # TODO: What is the right adapter between asyncio and threading event waits?
            await asyncio.sleep(0.05)
        return state

    event_watcher = asyncio.create_task(wait_for_final(final))

    try:
        ready.set()

        while not event_watcher.done():
            # TODO(#96): Use a control thread to manage *threading* primitives and
            #  translate to asyncio primitives.
            # Let the watcher wake up periodically to check for suspicious state.

            _rp_task_was_complete = task.state in rp.FINAL

            done, pending = await asyncio.wait([event_watcher], timeout=60.0, return_when=asyncio.FIRST_COMPLETED)

            if _rp_task_was_complete and not event_watcher.done():
                event_watcher.cancel()
                raise RPInternalError(
                    f"RP Callbacks are taking too long to complete. Abandoning {repr(task)}. Please report bug."
                )

            if event_watcher in done:
                assert final
                logger.debug(f"Handling finalization for RP task {task.uid}.")
                if final.failed.is_set():
                    # TODO(#92): Provide more useful error feedback.
                    raise RPTaskFailure(f"{task.uid} failed.", task=task)
                elif final.canceled.is_set():
                    # Act as if RP called Task.cancel() on us.
                    raise asyncio.CancelledError()
                assert final.done.is_set()

                logger.debug(f"Publishing results from RP Task {task.uid}.")
                # TODO: Manage result type.
                return task

        raise scalems.exceptions.InternalError("Logic error. This line should not have been reached.")

    except asyncio.CancelledError as e:
        logger.debug(f"Received cancellation in watcher task for {repr(task)}")
        if task.state not in rp.CANCELED:
            logger.debug(f"Propagating cancellation to {repr(task)}.")
            task.cancel()
        raise e


# TODO: Separate this out to a scalems.rptask module.
async def rp_task(rptask: rp.Task) -> asyncio.Task[rp.Task]:
    """Mediate between a radical.pilot.Task and an asyncio.Future.

    Schedule an asyncio Task to receive the result of the RP Task. The asyncio
    Task must also make sure that asyncio cancellation propagates to the rp.Task.cancel,
    and vice versa.

    This function should be awaited immediately to make sure the necessary call-backs
    get registered. The result will be an asyncio.Task, which should be awaited
    separately.

    Internally, this function provides a call-back to the rp.Task. The call-back
    provided to RP cannot directly call asyncio.Future methods (such as set_result() or
    set_exception()) because RP will be making the call from another thread without
    mediation by the asyncio event loop.

    As such, we provide a thread-safe event handler to propagate the
    RP Task call-back to to this asyncio.Task result.
    (See :py:func:`_rp_callback()` and :py:class:`RPFinalTaskState`)

    Canceling the returned task will cause *rptask* to be canceled.
    Canceling *rptask* will cause this task to be canceled.

    Arguments:
        rptask: RADICAL Pilot Task that has already been submitted.

    Returns:
        A Task that, when awaited, returns the rp.Task instance in its final state.
    """
    if not isinstance(rptask, rp.Task):
        raise TypeError("Function requires a RADICAL Pilot Task object.")

    final = RPFinalTaskState()
    callback = functools.partial(_rp_callback, final=final)
    functools.update_wrapper(callback, _rp_callback)

    # Note: register_callback() does not provide a return value to use for
    # TaskManager.unregister_callback and we cannot provide *callback* with a reference
    # to itself until after it is created, so we will get a reference here that we can
    # provide through the *cb_data* argument of rp.Task callbacks.
    cb_data: weakref.ReferenceType = weakref.ref(callback)

    rptask.register_callback(callback, cb_data=cb_data, metric=rp.constants.TASK_STATE)

    if rptask.state in rp.FINAL:
        # rptask may have reached FINAL state before callback was registered.
        # radical.pilot.Task.register_callback does not guarantee that callbacks
        # will be called if registered after task completion.
        # Call it once. For simplicity, let the task_watcher logic proceed normally.
        logger.warning(f"RP Task {repr(rptask)} finished suspiciously fast.")
        callback(rptask, rptask.state, cb_data)

    watcher_started = asyncio.Event()
    waiter = asyncio.create_task(watcher_started.wait())
    wrapped_task = asyncio.create_task(_rp_task_watcher(task=rptask, final=final, ready=watcher_started))

    # Make sure that the task is cancellable before returning it to the caller.
    await asyncio.wait((waiter, wrapped_task), return_when=asyncio.FIRST_COMPLETED)
    if wrapped_task.done():
        # Let CancelledError propagate.
        e = wrapped_task.exception()
        if e is not None:
            raise e
    # watcher_task.
    return wrapped_task


def _describe_legacy_task(item: scalems.workflow.Task, pre_exec: list) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    For a "raptor" style task, see _describe_raptor_task()
    """
    subprocess_type = TypeIdentifier(("scalems", "subprocess", "SubprocessTask"))
    assert item.description().type() == subprocess_type
    input_data = item.input
    task_input = scalems.subprocess.SubprocessInput(**input_data)
    args = list([arg for arg in task_input.argv])
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    task_description = rp.TaskDescription(
        from_dict=dict(
            executable=args[0],
            arguments=args[1:],
            stdout=str(task_input.stdout),
            stderr=str(task_input.stderr),
            pre_exec=pre_exec,
        )
    )
    uid: str = item.uid().hex()
    task_description.uid = uid

    # TODO: Check for and activate an appropriate venv
    # using
    #     task_description.pre_exec = ...
    # or
    #     task_description.named_env = ...

    # TODO: Interpret item details and derive appropriate staging directives.
    task_description.input_staging = list(task_input.inputs.values())
    task_description.output_staging = [
        {
            "source": str(task_input.stdout),
            "target": os.path.join(uid, pathlib.Path(task_input.stdout).name),
            "action": rp.TRANSFER,
        },
        {
            "source": str(task_input.stderr),
            "target": os.path.join(uid, pathlib.Path(task_input.stderr).name),
            "action": rp.TRANSFER,
        },
    ]
    task_description.output_staging += task_input.outputs.values()

    return task_description


async def submit(
    *,
    item: scalems.workflow.Task,
    task_manager: rp.TaskManager,
    pre_exec: list,
) -> asyncio.Task:
    """Dispatch a WorkflowItem to be handled by RADICAL Pilot.

    Submits an rp.Task and returns an asyncio.Task watcher for the submitted task.

    Creates a Future, registering a done_callback to publish the task result with
    *item.set_result()*.

    A callback is registered with the rp.Task to set an Event on completion. An
    asyncio.Task watcher task monitors the Event(s) and gets a reference to an
    asyncio.Future through which results can be published to the scalems workflow item.
    (Currently the Future is created in this function, but should probably be acquired
    directly from the *item* itself.) The watcher task waits for the rp.Task
    finalization event or for the Future to be cancelled. Periodically, the watcher
    task "wakes up" to check if something has gone wrong, such as the rp.Task
    completing without setting the finalization event.

    Caveats:
        There is an unavoidable race condition in the check performed by the watcher
        task. We don't know how long after an rp.Task completes before its callbacks
        will run and we can't check whether the callback has been scheduled. The
        watcher task cannot be allowed to complete successfully until we know that the
        callback has either run or will never run.

        The delay between the rp.Task state change and the callback execution should be
        less than a second. We can allow the watcher to wake up occasionally (on the
        order of minutes), so we can assume that it will never take more than a full
        iteration of the waiting loop for the callback to propagate, unless there is a
        bug in RP. For simplicity, we can just note whether ``rptask.state in rp.FINAL``
        before the watcher goes to sleep and raise an error if the callback is not
        triggered in an iteration where we have set such a flag.

        If our watcher sends a cancellation to the rp.Task, there is no need to
        continue to monitor the rp.Task state and the watcher may exit.

    Args:
        item: The workflow item to be submitted
        task_manager: A `radical.pilot.TaskManager` instance
            through which the task should be submitted.
        pre_exec: :py:data:`radical.pilot.Task.pre_exec` prototype.

    Returns:
        asyncio.Task: a "Future[rp.Task]" for a rp.Task in its final state.

    The caller *must* await the result of the coroutine to obtain an asyncio.Task that
    can be cancelled or awaited as a proxy to direct RP task management. The Task will
    hold a coroutine that is guaranteed to already be running, failed, or canceled. The
    caller should check the status of the task immediately before making assumptions
    about whether a Future has been successfully bound to the managed workflow item.

    The returned asyncio.Task can be used to cancel the rp.Task (and the Future)
    or to await the RP.Task cleanup.

    To submit tasks as a batch, await an array of submit() results in the
    same dispatching context. (TBD)
    """
    subprocess_type = TypeIdentifier(("scalems", "subprocess", "SubprocessTask"))
    submitted_type = item.description().type()
    if submitted_type == subprocess_type:
        rp_task_description = _describe_legacy_task(item, pre_exec=pre_exec)
    else:
        raise APIError(f"Cannot dispatch {submitted_type}.")

    # TODO(#249): A utility function to move slow blocking RP calls to a separate thread.
    #  Compartmentalize TaskDescription -> rp_task_watcher in a separate utility function.
    task = task_manager.submit_tasks(rp_task_description)

    rp_task_watcher = await rp_task(rptask=task)

    if rp_task_watcher.done():
        if rp_task_watcher.cancelled():
            raise DispatchError(f"Task for {item} was unexpectedly canceled during dispatching.")
        e = rp_task_watcher.exception()
        if e is not None:
            raise DispatchError("Task for {item} failed during dispatching.") from e

    # Warning: in the long run, we should not extend the life of the reference returned
    # by edit_item, and we need to consider the robust way to publish item results.
    # TODO: Translate RP result to item result type.
    rp_task_watcher.add_done_callback(functools.partial(scalems_callback, item=item))
    # TODO: If *item* acquires a `cancel` method, we need to subscribe to it and
    #  respond by unregistering the callback and canceling the future.

    return rp_task_watcher


def scalems_callback(fut: asyncio.Future, *, item: scalems.workflow.Task):
    """Propagate the results of a Future to the subscribed *item*.

    Partially bind *item* to use this as the argument to *fut.add_done_callback()*.

    Warning:
        In the long run, we should not extend the life of the reference returned
        by edit_item, and we need to consider the robust way to publish item results.

    Note:
        This is not currently RP-specific and we should look at how best to factor
        results publishing for workflow items. It may be that this function is the
        appropriate place to convert RP Task results to scalems results.
    """
    assert fut.done()
    if fut.cancelled():
        logger.info(f"Task supporting {item} has been cancelled.")
    else:
        # The following should not throw because we just checked for `done()` and
        # `cancelled()`
        e = fut.exception()
        if e:
            logger.info(f"Task supporting {item} failed: {e}")
        else:
            # TODO: Construct an appropriate scalems Result from the rp Task.
            item.set_result(fut.result())


class WorkflowUpdater(AbstractWorkflowUpdater):
    def __init__(self, executor: RPDispatchingExecutor):
        self.executor = executor
        self.task_manager = executor.runtime.task_manager()
        # TODO: Make sure we are clear about the scope of the configuration and the
        #  life time of the workflow updater / submitter.
        self._pre_exec = list(get_pre_exec(executor.configuration()))

    async def submit(self, *, item: scalems.workflow.Task) -> asyncio.Task:
        # TODO: Ensemble handling
        item_shape = item.description().shape()
        if len(item_shape) != 1 or item_shape[0] != 1:
            raise MissingImplementationError("Executor cannot handle multidimensional tasks yet.")

        task: asyncio.Task[rp.Task] = await submit(item=item, task_manager=self.task_manager, pre_exec=self._pre_exec)
        return task
