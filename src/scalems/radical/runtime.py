"""Manage the RADICAL Pilot start-up and shut-down.

The provided Runtime class encapsulates stateful resources that, once acquired,
should be shut down explicitly. Runtime instances may be used as context managers to
ensure the proper protocol is followed, or the caller can take responsibility for
calling Runtime.close() to shut down.

Note: Consider whether Runtime context manager is reentrant or multi-use.

The Runtime state encapsulates several nested states. A Session, TaskManager,
and PilotManager must be created for the Runtime to be usable. Additionally, Pilots and
scheduler tasks may be added or removed during the Runtime lifetime. To better support
the alternate scenarios when a Runtime instance may be provided to a scalems.radical
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

"""
import argparse
import asyncio
import contextvars
import dataclasses
import functools
import logging
import os
import typing
import warnings

import packaging.version
from radical import pilot as rp

import scalems.utility as _utility
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import InternalError

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

# TODO: Consider scoping for these context variables.
# Need to review PEP-567 and PEP-568 to consider where and how to scope the Context
# with respect to the dispatching scope.
_configuration = contextvars.ContextVar('_configuration')

try:
    cache = functools.cache
except AttributeError:
    # Note: functools.cache does not appear until Python 3.9
    cache = functools.lru_cache(maxsize=None)


def _parse_option(arg: str) -> tuple:
    if not isinstance(arg, str):
        raise InternalError('Bug: This function should only be called with a str.')
    if arg.count('=') != 1:
        raise argparse.ArgumentTypeError('Expected a key/value pair delimited by "=".')
    return tuple(arg.split('='))


@cache
def parser(add_help=False):
    """Get the module-specific argument parser.

    Provides a base argument parser for scripts using the scalems.radical backend.

    By default, the returned ArgumentParser is created with ``add_help=False``
    to avoid conflicts when used as a *parent* for a parser more local to the caller.
    If *add_help* is provided, it is passed along to the ArgumentParser created
    in this function.

    See Also:
         https://docs.python.org/3/library/argparse.html#parents
    """
    _parser = argparse.ArgumentParser(add_help=add_help, parents=[_utility.parser()])

    # We could consider inferring a default venv from the VIRTUAL_ENV environment
    # variable,
    # but we currently have very poor error handling regarding venvs. For now, this needs
    # to be explicit.
    # Ref https://github.com/SCALE-MS/scale-ms/issues/89
    # See also https://github.com/SCALE-MS/scale-ms/issues/90
    # TODO: Set module variables rather than carry around an args namespace?
    _parser.add_argument('--venv',
                         metavar='PATH',
                         type=str,
                         required=True,
                         help='tasks.Path to the (pre-configured) Python virtual '
                              'environment with which RP tasks should be executed. '
                              '(Required. See also https://github.com/SCALE-MS/scale-ms/issues/90)')

    _parser.add_argument(
        '--resource',
        type=str,
        required=True,
        help='Specify a `RP resource` for the radical.pilot.PilotDescription. (Required)'
    )

    _parser.add_argument(
        '--access',
        type=str,
        help='Explicitly specify the access_schema to use from the RADICAL resource.'
    )

    _parser.add_argument(
        '--pilot-option',
        action='append',
        type=_parse_option,
        metavar='<key>=<value>',
        help='Add a key value pair to the `radical.pilot.PilotDescription`.'
    )
    return _parser


@dataclasses.dataclass(frozen=True)
class Configuration:
    """Module configuration information.

    See also:
        :py:func:`scalems.radical.configuration`
        :py:data:`scalems.radical.parser`
        :py:class:`scalems.radical.runtime.Runtime`

    TODO: Consider merging with module Runtime state container.
    """
    # Note that the use cases for this dataclass interact with module ContextVars,
    # pending refinement.
    # TODO: Check that the resource is defined.
    execution_target: str = 'local.localhost'
    rp_resource_params: dict = dataclasses.field(default_factory=dict)
    target_venv: str = None


class Runtime:
    """Container for scalems.radical runtime state data.

    TODO: Consider merging with `scalems.radical.Configuration`

    See Also:
        scalems.radical.RPDispatchingExecutor.runtime()
        scalems.radical._connect_rp()

    """
    _session: rp.Session
    scheduler: typing.Optional[rp.Task] = None

    _pilot_manager: typing.Optional[rp.PilotManager] = None
    _pilot: typing.Optional[rp.Pilot] = None
    _task_manager: typing.Optional[rp.TaskManager] = None

    def __init__(self, session: rp.Session):
        if not isinstance(session, rp.Session) or session.closed:
            raise ValueError('*session* must be an active RADICAL Pilot Session.')
        self._session = session

    def reset(self, session: rp.Session):
        """Reset the runtime state.

        Close any existing resources and revert to a new Runtime state containing only
        the provided *session*.
        """
        if not isinstance(session, rp.Session) or session.closed:
            raise ValueError('*session* must be an active RADICAL Pilot Session.')
        self._session.close()
        # Warning: This is not quite right.
        # The attribute values are deferred to the class dict from initialization. The
        # following lines actually leave the instance members in place with None values
        # rather than removing them, but the logic of checking for and removing the
        # instance values seems a little harder to read.
        self.scheduler = None
        self._pilot = None
        self._task_manager = None
        self._pilot_manager = None
        self._session = session

    @property
    def session(self) -> rp.Session:
        return self._session

    @typing.overload
    def pilot_manager(self) -> typing.Union[rp.PilotManager, None]:
        """Get the current PilotManager, if any."""
        ...

    @typing.overload
    def pilot_manager(self, pilot_manager: str) -> typing.Union[rp.PilotManager, None]:
        """Set the pilot manager from a UID"""
        ...

    @typing.overload
    def pilot_manager(self, pilot_manager: rp.PilotManager) \
            -> typing.Union[rp.PilotManager, None]:
        """Set the current pilot manager as provided."""
        ...

    def pilot_manager(self, pilot_manager=None) -> typing.Union[rp.PilotManager, None]:
        if pilot_manager is None:
            return self._pilot_manager
        elif isinstance(pilot_manager, rp.PilotManager):
            if not pilot_manager.session.uid == self.session.uid:
                raise APIError('Cannot accept a PilotManager from a different Session.')
            self._pilot_manager = pilot_manager
            return pilot_manager
        else:
            uid = pilot_manager
            try:
                pmgr = self.session.get_pilot_managers(pmgr_uids=uid)
                assert isinstance(pmgr, rp.PilotManager)
            except (AssertionError, KeyError) as e:
                raise ValueError(f'{uid} does not describe a valid PilotManager') from e
            except Exception as e:
                logger.exception('Unhandled RADICAL Pilot exception.', exc_info=e)
                raise ValueError(f'{uid} does not describe a valid PilotManager') from e
            else:
                return self.pilot_manager(pmgr)

    @typing.overload
    def task_manager(self) -> typing.Union[rp.TaskManager, None]:
        """Get the current TaskManager, if any."""
        ...

    @typing.overload
    def task_manager(self, task_manager: str) -> typing.Union[rp.TaskManager, None]:
        """Set the TaskManager from a UID."""
        ...

    @typing.overload
    def task_manager(self, task_manager: rp.TaskManager) \
            -> typing.Union[rp.TaskManager, None]:
        """Set the TaskManager from the provided instance."""
        ...

    def task_manager(self, task_manager=None) -> typing.Union[rp.TaskManager, None]:
        if task_manager is None:
            return self._task_manager
        elif isinstance(task_manager, rp.TaskManager):
            if not task_manager.session.uid == self.session.uid:
                raise APIError('Cannot accept a TaskManager from a different Session.')
            self._task_manager = task_manager
            return task_manager
        else:
            uid = task_manager
            try:
                tmgr = self.session.get_task_managers(tmgr_uids=uid)
                assert isinstance(tmgr, rp.TaskManager)
            except (AssertionError, KeyError) as e:
                raise ValueError(f'{uid} does not describe a valid TaskManager') from e
            except Exception as e:
                logger.exception('Unhandled RADICAL Pilot exception.', exc_info=e)
                raise ValueError(f'{uid} does not describe a valid TaskManager') from e
            else:
                return self.task_manager(tmgr)

    @typing.overload
    def pilot(self) -> typing.Union[rp.Pilot, None]:
        """Get the current Pilot, if any."""
        ...

    @typing.overload
    def pilot(self, pilot: str) -> typing.Union[rp.Pilot, None]:
        """Set the Pilot according to the provided UID."""
        ...

    @typing.overload
    def pilot(self, pilot: rp.Pilot) -> typing.Union[rp.Pilot, None]:
        """Set the Pilot to the provided instance."""
        ...

    def pilot(self, pilot=None) -> typing.Union[rp.Pilot, None]:
        """Get (optionally set) the current Pilot."""
        if pilot is None:
            return self._pilot

        pmgr = self.pilot_manager()
        if not pmgr:
            raise APIError('Cannot set Pilot before setting PilotManager.')

        if isinstance(pilot, rp.Pilot):
            session = pilot.session
            if not isinstance(session, rp.Session):
                raise APIError(f'Pilot {repr(pilot)} does not have a valid Session.')
            if session.uid != self.session.uid:
                raise APIError('Cannot accept a Pilot from a different Session.')
            if pilot.pmgr.uid != pmgr.uid:
                raise APIError('Pilot must be associated with a PilotManager '
                               'already configured.')
            self._pilot = pilot
            return pilot
        else:
            uid = pilot
            try:
                pilot = pmgr.get_pilots(uids=uid)
                assert isinstance(pilot, rp.Pilot)
            except (AssertionError, KeyError, ValueError) as e:
                raise ValueError(f'{uid} does not describe a valid Pilot') from e
            except Exception as e:
                # TODO: Track down the expected rp exception.
                logger.exception('Unhandled RADICAL Pilot exception.', exc_info=e)
                raise ValueError(f'{uid} does not describe a valid Pilot') from e
            else:
                return self.pilot(pilot)


def _connect_rp(config: Configuration) -> Runtime:
    """Establish the RP Session.

    Acquire as many re-usable resources as possible. The scope established by
    this function is as broad as it can be within the life of this instance.

    Once instance._connect_rp() succeeds, instance._disconnect_rp() must be called to
    clean up resources. Use the async context manager behavior of the instance to
    automatically follow this protocol. I.e. instead of calling
    ``instance._connect_rp(); ...; instance._disconnect_rp()``,
    use::
        async with instance:
            ...

    Raises:
        DispatchError if task dispatching could not be set up.

        CanceledError if parent asyncio.Task is cancelled while executing.

    """
    # TODO: Consider inlining this into __aenter__().
    # A non-async method is potentially useful for debugging, but causes the event loop
    # to block while waiting for the RP tasks included here. If this continues to be a
    # slow function, we can wrap the remaining RP calls and let this function be
    # inlined, or stick the whole function in a separate thread with
    # loop.run_in_executor().

    # TODO: RP triggers SIGINT in various failure modes.
    #  We should use loop.add_signal_handler() to convert to an exception
    #  that we can raise in an appropriate task.
    # Note that PilotDescription can use `'exit_on_error': False` to suppress the SIGINT,
    # but we have not explored the consequences of doing so.

    try:
        #
        # Start the Session.
        #

        # Note that we cannot resolve the full _resource config until we have a Session
        # object.
        # We cannot get the default session config until after creating the Session,
        # so we don't have a template for allowed, required, or default values.
        # Question: does the provided *cfg* need to be complete? Or will it be merged
        # with default values from some internal definition, such as by dict.update()?
        # I don't remember what the use cases are for overriding the default session
        # config.
        session_config = None
        # At some point soon, we need to track Session ID for the workflow metadata.
        # We may also want Session ID to be deterministic (or to be re-used?).
        session_id = None

        # Note: the current implementation implies that only one Task for the dispatcher
        # will exist at a time. We are further assuming that there will probably only
        # be one Task per the lifetime of the dispatcher object.
        # We could choose another approach and change our assumptions, if appropriate.
        logger.debug('Entering RP dispatching context. Waiting for rp.Session.')

        # Note: radical.pilot.Session creation causes several deprecation warnings.
        # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=DeprecationWarning)
            # This would be a good time to `await`, if an event-loop friendly
            # Session creation function becomes available.
            runtime = Runtime(session=rp.Session(uid=session_id, cfg=session_config))
        session_id = runtime.session.uid
        # Do we want to log this somewhere?
        # session_config = copy.deepcopy(self.session.cfg.as_dict())
        logger.debug('RP dispatcher acquired session {}'.format(session_id))

        # We can launch an initial Pilot, but we may have to run further Pilots
        # during self._queue_runner_task (or while servicing scalems.wait() within the
        # with block) to handle dynamic work load requirements.
        # Optionally, we could refrain from launching the pilot here, at all,
        # but it seems like a good chance to start bootstrapping the agent environment.
        logger.debug('Launching PilotManager.')
        pilot_manager = rp.PilotManager(session=runtime.session)
        logger.debug('Got PilotManager {}.'.format(pilot_manager.uid))
        runtime.pilot_manager(pilot_manager)

        logger.debug('Launching TaskManager.')
        task_manager = rp.TaskManager(session=runtime.session)
        logger.debug(('Got TaskManager {}'.format(task_manager.uid)))
        runtime.task_manager(task_manager)

        #
        # Get a Pilot
        #

        # # TODO: #94 Describe (link to) configuration points.
        # resource_config['local.localhost'].update({
        #     'project': None,
        #     'queue': None,
        #     'schema': None,
        #     'cores': 1,
        #     'gpus': 0
        # })

        # _pilot_description = dict(_resource=_resource,
        #                          runtime=30,
        #                          exit_on_error=True,
        #                          project=resource_config[_resource]['project'],
        #                          queue=resource_config[_resource]['queue'],
        #                          cores=resource_config[_resource]['cores'],
        #                          gpus=resource_config[_resource]['gpus'])

        # TODO: How to specify PilotDescription? (see also #121)
        # Where should this actually be coming from?
        # We need to inspect both the HPC allocation and the work load, I think,
        # and combine with user-provided preferences.
        pilot_description = {}
        pilot_description.update(config.rp_resource_params.get('PilotDescription', {}))
        pilot_description.update({'resource': config.execution_target})

        # TODO: Pilot venv (#90, #94).
        # Currently, Pilot venv must be specified in the JSON file for resource
        # definitions.
        pilot_description = rp.PilotDescription(pilot_description)

        # How and when should we update pilot description?
        logger.debug('Submitting PilotDescription {}'.format(repr(
            pilot_description)))
        pilot = pilot_manager.submit_pilots(pilot_description)
        logger.debug('Got Pilot {}'.format(pilot.uid))
        runtime.pilot(pilot)

        # Note that the task description for the master (and worker) can specify a
        # *named_env* attribute to use a venv prepared via Pilot.prepare_env
        # E.g.         pilot.prepare_env({'numpy_env' : {'type'   : 'virtualenv',
        #                                           'version': '3.6',
        #                                           'setup'  : ['numpy']}})
        #   td.named_env = 'numpy_env'
        # Note that td.named_env MUST be a key that is given to pilot.prepare_env(arg:
        # dict) or the task will wait indefinitely to be scheduled.
        # Alternatively, we could use a pre-installed venv by putting
        # `. path/to/ve/bin/activate`
        # in the TaskDescription.pre_exec list.

        # TODO: Use archives generated from (acquired through) the local installations.
        # # Could we stage in archive distributions directly?
        # # self.pilot.stage_in()
        # pilot.prepare_env(
        #     {
        #         'scalems_env': {
        #             'type': 'virtualenv',
        #             'version': '3.8',
        #             'setup': [
        #                 # TODO: Generalize scalems dependency resolution.
        #                 # Ideally, we would check the current API version
        #                 # requirement, map that to a package version,
        #                 # and specify >=min_version, allowing cached archives to
        #                 # satisfy the dependency.
        #                 rp_spec,
        #                 scalems_spec
        #             ]}})

        # Question: when should we remove the pilot from the task manager?
        task_manager.add_pilots(pilot)
        logger.debug('Added Pilot {} to task manager {}.'.format(
            pilot.uid,
            task_manager.uid))

        pre_exec = get_pre_exec(config)
        assert isinstance(pre_exec, tuple)
        assert len(pre_exec) > 0
        # Verify usable SCALEMS RP connector.
        # TODO: Fetch a profile of the venv for client-side analysis (e.g. `pip freeze`).
        # TODO: Check for compatible installed scalems API version.
        rp_check = task_manager.submit_tasks(rp.TaskDescription({
            # 'executable': py_venv,
            'executable': 'python3',
            'arguments': ['-c', 'import radical.pilot as rp; print(rp.version)'],
            'pre_exec': list(pre_exec)
            # 'named_env': 'scalems_env'
        }))
        logger.debug('Checking RP execution environment.')
        states = task_manager.wait_tasks(uids=[rp_check.uid])
        if states[0] != rp.states.DONE or rp_check.exit_code != 0:
            raise DispatchError('Could not verify RP in execution environment.')

        try:
            remote_rp_version = packaging.version.parse(rp_check.stdout.rstrip())
        except Exception as e:
            raise DispatchError('Could not determine remote RP version.') from e
        # TODO: #100 Improve compatibility checking.
        if remote_rp_version < packaging.version.parse('1.6.0'):
            raise DispatchError(f'Incompatible radical.pilot version in execution '
                                f'environment: {str(remote_rp_version)}')

        #
        # Get a scheduler task.
        #

        assert runtime.scheduler is None
        # TODO: #119 Re-enable raptor.
        # runtime.scheduler = _get_scheduler(
        #     'raptor.scalems',
        #     pre_exec=execution_manager._pre_exec,
        #     task_manager=task_manager)
        # Note that we can derive scheduler_name from self.scheduler.uid in later methods.
        # Note: The worker script name only appears in the config file.
        # logger.info('RP scheduler ready.')
        # logger.debug(repr(execution_manager.scheduler))

        return runtime

    except asyncio.CancelledError as e:
        raise e
    except Exception as e:
        logger.exception('Exception while connecting RADICAL Pilot.', exc_info=e)
        raise DispatchError('Failed to launch SCALE-MS master task.') from e


# functools can't cache this function while Configuration is unhashable (due to
# unhashable dict member).
# @cache
def get_pre_exec(conf: Configuration) -> tuple:
    """Get the sequence of pre_exec commands.

    Warning:
        Use cases may require a `list` object. Caller is responsible for converting
        the returned tuple if appropriate.

    """
    if conf.target_venv is None or len(conf.target_venv) == 0:
        raise ValueError(
            'Currently, tasks cannot be dispatched without a target venv.')

    activate_venv = '. ' + str(os.path.join(conf.target_venv, 'bin', 'activate'))
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
    """
    assert len(args) != 0 or len(kwargs) != 0
    # Caller has provided arguments.
    # Not thread-safe
    if _configuration.get(None):
        raise APIError(f'configuration() cannot accept arguments when {__name__} is '
                       f'already configured.')
    c = Configuration(*args, **kwargs)
    _configuration.set(c)
    return _configuration.get()


@_set_configuration.register
def _(config: Configuration) -> Configuration:
    # Not thread-safe
    if _configuration.get(None):
        raise APIError(f'configuration() cannot accept arguments when {__name__} is '
                       f'already configured.')
    _configuration.set(config)
    return _configuration.get()


@_set_configuration.register
def _(namespace: argparse.Namespace) -> Configuration:
    rp_resource_params = {
        'PilotDescription':
            {
                'access_schema': namespace.access
            }
    }
    if namespace.pilot_option is not None and len(namespace.pilot_option) > 0:
        logger.debug(f'Pilot options: {repr(namespace.pilot_option)}')
        rp_resource_params.update(namespace.pilot_option)

    config = Configuration(
        execution_target=namespace.resource,
        target_venv=namespace.venv,
        rp_resource_params=rp_resource_params
    )
    return _set_configuration(config)
