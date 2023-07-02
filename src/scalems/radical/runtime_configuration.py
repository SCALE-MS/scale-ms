from __future__ import annotations

__all__ = (
    "configuration",
    "get_pre_exec",
    "parser",
    "RuntimeConfiguration",
    "RPResourceParams",
)

import argparse
import collections.abc
import dataclasses
import functools
import logging
import os
import typing
import warnings

from scalems.radical.exceptions import RPConfigurationError
from radical import pilot as rp

import scalems.invocation
from scalems.exceptions import InternalError
from scalems.exceptions import MissingImplementationError

if typing.TYPE_CHECKING:
    from radical import utils as ru

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


def _parse_option(arg: str) -> tuple:
    if not isinstance(arg, str):
        raise InternalError("Bug: This function should only be called with a str.")
    if arg.count("=") != 1:
        raise argparse.ArgumentTypeError('Expected a key/value pair delimited by "=".')
    return tuple(arg.split("="))


@functools.cache
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
    _parser = argparse.ArgumentParser(add_help=add_help, parents=[scalems.invocation.base_parser()])

    # We could consider inferring a default venv from the VIRTUAL_ENV environment
    # variable,
    # but we currently have very poor error handling regarding venvs. For now, this needs
    # to be explicit.
    # Ref https://github.com/SCALE-MS/scale-ms/issues/89
    # See also https://github.com/SCALE-MS/scale-ms/issues/90
    # TODO: Set module variables rather than carry around an args namespace?

    _parser.add_argument(
        "--access",
        type=str,
        help="Explicitly specify the access_schema to use from the RADICAL resource.",
    )

    _parser.add_argument(
        "--enable-raptor",
        action="store_true",
        help="Enable RP Raptor, and manage an execution side dispatching task.",
    )

    _parser.add_argument(
        "--pilot-option",
        action="append",
        type=_parse_option,
        metavar="<key>=<value>",
        help="Add a key value pair to the `radical.pilot.PilotDescription`.",
    )

    _parser.add_argument(
        "--resource",
        type=str,
        required=True,
        help="Specify a `RP resource` for the radical.pilot.PilotDescription. (Required)",
    )

    _parser.add_argument(
        "--venv",
        metavar="PATH",
        type=str,
        required=True,
        help="Path to the (pre-configured) Python virtual "
        "environment with which RP tasks should be executed. "
        "(Required. See also https://github.com/SCALE-MS/scale-ms/issues/90)",
    )

    return _parser


@dataclasses.dataclass(frozen=True)
class RuntimeConfiguration:
    """Module configuration information.

    See also:
        * :py:func:`scalems.radical.runtime_configuration.configuration()`
        * :py:data:`scalems.radical.runtime.parser`
        * :py:class:`scalems.radical.runtime.RuntimeSession`
    """

    execution_target: str = "local.localhost"
    """Platform identifier for the RADCIAL Pilot execution resource."""

    rp_resource_params: RPResourceParams = dataclasses.field(default_factory=dict)
    """Schema for this member container may not be stable."""

    target_venv: str = None
    """Path to a pre-configured Python virtual environment on *execution_target*."""

    enable_raptor: bool = False

    def __post_init__(self):
        if "PilotDescription" not in self.rp_resource_params:
            self.rp_resource_params["PilotDescription"] = dict()

        if "exit_on_error" not in self.rp_resource_params["PilotDescription"]:
            self.rp_resource_params["PilotDescription"]["exit_on_error"] = False
        elif self.rp_resource_params["PilotDescription"]["exit_on_error"]:
            warnings.warn("Allowing RP Pilot to exit_on_error can prevent scalems from shutting down cleanly.")

        hpc_platform_label = self.execution_target
        access = self.rp_resource_params["PilotDescription"].get("access_schema")
        try:
            job_endpoint: ru.Url = rp.utils.misc.get_resource_job_url(hpc_platform_label, schema=access)
        except (TypeError, KeyError) as e:
            raise RPConfigurationError(f"Could not resolve {access} access for {hpc_platform_label}") from e

        if self.enable_raptor:
            # scalems uses the RP MPIWorker, which can have problems in "local" execution modes.
            launch_scheme = job_endpoint.scheme
            if launch_scheme == "fork":
                message = f"RP Raptor MPI Worker not supported for '{launch_scheme}' launch method."
                message += f" '{access}' access for {hpc_platform_label}: {job_endpoint}"
                raise RPConfigurationError(message)


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


class RPResourceParams(typing.TypedDict):
    PilotDescription: dict
    """A dictionary representation of a PilotDescription.

    See :py:class:`radical.pilot.PilotDescription` for allowed fields and value types.
    """


def configuration(*args, **kwargs) -> RuntimeConfiguration:
    """Get a RADICAL runtime configuration.

    With no arguments, the command line parser is invoked to try to build a new
    configuration.

    If arguments are provided, try to construct a `scalems.radical.runtime.RuntimeConfiguration`.

    See Also:
        :py:func:`current_configuration()` retrieves the configuration for an active
        RuntimeManager, if any.
    """
    from scalems.radical import parser

    # Warning: (bool(args) or bool(kwargs)) != (args or kwargs).
    # Using `len` for readability.
    if len(args) > 0 or len(kwargs) > 0:
        config = RuntimeConfiguration(*args, **kwargs)
    else:
        namespace, _ = parser.parse_known_args()
        rp_resource_params = {"PilotDescription": {"access_schema": namespace.access}}
        if namespace.pilot_option is not None and len(namespace.pilot_option) > 0:
            user_options = _PilotDescriptionProxy.normalize_values(namespace.pilot_option)
            rp_resource_params["PilotDescription"].update(user_options)
            logger.debug(f'Pilot options: {repr(rp_resource_params["PilotDescription"])}')

        config = RuntimeConfiguration(
            execution_target=namespace.resource,
            target_venv=namespace.venv,
            rp_resource_params=rp_resource_params,
            enable_raptor=namespace.enable_raptor,
        )
    if config.enable_raptor:
        logger.debug("RP Raptor enabled.")
    else:
        logger.debug("RP Raptor disabled.")

    return config


# functools can't cache this function while Configuration is unhashable (due to
# unhashable dict member).
# @functools.cache
def get_pre_exec(conf: RuntimeConfiguration) -> tuple:
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
