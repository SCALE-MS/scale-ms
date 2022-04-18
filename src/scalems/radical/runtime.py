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

from __future__ import annotations

__all__ = (
    'parser',
    'Configuration',
    'Runtime',
)

import argparse
import asyncio
import contextvars
import dataclasses
import functools
import json
import logging
import os
import tempfile
import typing

from radical import pilot as rp

import scalems.radical.raptor
import scalems.utility as _utility
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import InternalError
from .common import RaptorWorkerConfig
from .common import worker_description
from .raptor import master_script
from .raptor import object_encoder
from .. import FileReference
from ..context import describe_file
from ..context import FileStore
from ..identifiers import EphemeralIdentifier

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
        * :py:func:`scalems.radical.configuration`
        * :py:data:`scalems.radical.runtime.parser`
        * :py:class:`scalems.radical.runtime.Runtime`

    TODO: Consider merging with module Runtime state container.
    """
    # Note that the use cases for this dataclass interact with module ContextVars,
    # pending refinement.
    datastore: FileStore = None
    # TODO: Check that the resource is defined.
    execution_target: str = 'local.localhost'
    rp_resource_params: dict = dataclasses.field(default_factory=dict)
    target_venv: str = None


class Runtime:
    """Container for scalems.radical runtime state data.

    TODO: Consider merging with `scalems.radical.Configuration`

    See Also:
        * :py:attr:`scalems.radical.RPDispatchingExecutor.runtime`
        * :py:func:`scalems.radical.runtime._connect_rp()`

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
        """Get (optionally set) the current Pilot.

        Args
        ----
        pilot : radical.pilot.Pilot, str, None
            Set to RP Pilot instance or identifier, if provided.

        Returns
        -------
        radical.pilot.Pilot
            instance
        """
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


async def _master_input(filestore: FileStore, pre_exec: list, named_env: str) -> FileReference:
    """Provide the input file for a SCALE-MS Raptor Master script.

    Args:
        filestore: (local) FileStore that will manage the generated FileReference.

    """
    if not isinstance(filestore, FileStore) or filestore.closed or not \
            filestore.directory.exists():
        raise ValueError(f'{filestore} is not a usable FileStore.')

    # Worker tasks may not appear unique, but must be uniquely identified within the
    # scope of a rp.Session for RP bookkeeping. Since there is no other interesting
    # information at this time, we can generate a random ID and track it in our metadata.
    worker_identity = EphemeralIdentifier()
    task_metadata = {
        'uid': str(worker_identity),
        'pre_exec': pre_exec
    }
    filestore.add_task(worker_identity, **task_metadata)
    # This is the initial Worker submission. The Master may submit other workers later,
    # but we should try to make this one as usable as possible.
    # TODO: Inspect workflow to optimize reusability of the initial Worker submission.
    num_workers = 1
    cores_per_worker = 1
    gpus_per_worker = 0
    task_metadata.update(
        worker_description(
            cpu_processes=cores_per_worker,
            gpu_processes=gpus_per_worker,
            named_env=named_env))

    # TODO: Add additional dependencies that we can infer from the workflow.
    versioned_modules = (
        ('scalems', scalems.__version__),
        ('radical.pilot', rp.version)
    )

    configuration = scalems.radical.raptor.Configuration(
        worker=RaptorWorkerConfig(
            descr=task_metadata,
            count=num_workers
        ),
        versioned_modules=list(versioned_modules)
    )

    # Make sure the temporary directory is on the same filesystem as the local workflow.
    tmp_base = filestore.directory
    with tempfile.TemporaryDirectory(dir=tmp_base) as dir:
        config_file_name = 'raptor_scheduler_config.json'
        config_file_path = os.path.join(dir, config_file_name)
        with open(config_file_path, 'w') as fh:
            json.dump(configuration, fh, default=object_encoder, indent=2)
        file_description = describe_file(config_file_path, mode='r')
        handle: FileReference = await asyncio.create_task(
            filestore.add_file(file_description),
            name='add-file')
    return handle


async def _get_scheduler(pre_exec: typing.Iterable[str],
                         task_manager: rp.TaskManager,
                         filestore: FileStore,
                         scalems_env: str):
    """Establish the radical.pilot.raptor.Master task.

    Create a master rp.Task (running the scalems_rp_master script) with the
    provide *name* to be referenced as the *scheduler* for raptor tasks.

    Returns the rp.Task for the master script once the Master is ready to
    receive submissions.

    Raises:
        DispatchError if the master task could not be launched successfully.

    Note:
        Currently there is no completion condition for the master script.
        Caller is responsible for canceling the Task returned by this function.
    """
    # define a raptor.scalems master and launch it within the pilot
    td = rp.TaskDescription()

    td.pre_exec = pre_exec
    td.stage_on_error = True
    # We are not using prepare_env at this point. We use the `venv` configured by the
    # caller.
    # td.named_env = 'scalems_env'

    # This is the name that should be resolvable in an active venv for the script we
    # install as
    # pkg_resources.get_entry_info('scalems', 'console_scripts', 'scalems_rp_master').name
    td.executable = master_script()

    logger.debug(f'Using {filestore}.')

    # scalems_rp_master will write output before it begins handling requests. The
    # script may crash even before it can write anything, but if it does write
    # anything, we _will_ have the output file locally
    td.output_staging = [

    ]

    # _original_callback_duration = asyncio.get_running_loop().slow_callback_duration
    # asyncio.get_running_loop().slow_callback_duration = 0.5
    config_file = await asyncio.create_task(
        _master_input(filestore,
                      pre_exec=list(pre_exec),
                      named_env=scalems_env),
        name='get-master-input'
    )
    # asyncio.get_running_loop().slow_callback_duration = _original_callback_duration

    # TODO(#75): Automate handling of file staging directives for scalems.FileReference
    # e.g. _add_file_dependency(td, config_file)
    config_file_name = config_file.path().name
    td.input_staging = [
        {
            'source': config_file.as_uri(),
            'target': f'task://{config_file_name}',
            'action': rp.TRANSFER
        }
    ]
    td.arguments = [config_file_name]

    # Master tasks may not appear unique, but must be uniquely identified within the
    # scope of a rp.Session for RP bookkeeping. Since there is no other interesting
    # information at this time, we can generate a random ID and track it in our metadata.
    master_identity = EphemeralIdentifier()
    td.uid = str(master_identity)
    task_metadata = {
        'uid': td.uid,
        'task_manager': task_manager.uid
    }

    to_thread = _utility.get_to_thread()

    await asyncio.create_task(
        to_thread(filestore.add_task, master_identity, **task_metadata),
        name='add-task'
    )
    # filestore.add_task(master_identity, **task_metadata)

    logger.debug(f'Launching RP raptor scheduling. Submitting {td}.')

    _task = asyncio.create_task(to_thread(task_manager.submit_tasks, td),
                                name='submit-Master')
    scheduler: rp.Task = await _task

    # WARNING: rp.Task.wait() *state* parameter does not handle tuples, but does not
    # check type.
    _task = asyncio.create_task(
        to_thread(scheduler.wait, state=[rp.states.AGENT_EXECUTING] + rp.FINAL),
        name='check-Master-started'
    )
    await _task
    logger.debug(f'Scheduler in state {scheduler.state}. Proceeding.')
    # TODO: Generalize the exit status checker for the Master task and perform this
    #  this check at the call site.
    if scheduler.state in rp.FINAL:
        if scheduler.stdout or scheduler.stderr:
            logger.error(f'scheduler.stdout: {scheduler.stdout}')
            logger.error(f'scheduler.stderr: {scheduler.stderr}')
        raise DispatchError(
            f'Master Task unexpectedly reached {scheduler.state} during launch.')
    return scheduler


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
                'access_schema': namespace.access,
                'exit_on_error': False,
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
