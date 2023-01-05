"""
Gromacs simulation tools.

Preparation and output manipulation use command line tools.
Simulation is executed with gmxapi.
"""
# Declare the public interface of this wrapper module.
__all__ = ["make_input", "modify_input", "simulate"]

##########################
# New plan:
# Implement basic gmxapi examples directly without generalism or type checking:
# Create simple dictionaries and add them to the workflow manager.
# 0. Write gromacs wrapper in terms of dicts. Processing is done via scalems entry points.
# 1. Hard-code handling in WorkflowManager.
# 2. Resume handling of dict and JSON representations in fingerprint and serialization
# modules.
# 3. Refine Python data model and build framework.
###########################
import functools
import logging

# import gmxapi
import os
import pathlib
import typing
from typing import Sequence

import scalems.workflow
from scalems.exceptions import MissingImplementationError
from scalems.unique import next_monotonic_integer as _next_int
from scalems.workflow import WorkflowManager

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


# TODO: REMOVE. Follow with fingerprint based UID ASAP.
def _next_uid() -> bytes:
    # Note that network byte order is equivalent to "big endian".
    return _next_int().to_bytes(32, "big")


def _executable(
    argv: Sequence[str],
    inputs: Sequence[str] = (),
    outputs: Sequence[str] = (),
    stdin: typing.Iterable[str] = (),
    resources: typing.Mapping[str, str] = None,
    environment: typing.Mapping[str, str] = None,
    stdout: str = "stdout.txt",
    stderr: str = "stderr.txt",
):
    if resources is None:
        resources = {}
    if environment is None:
        environment = {}

    input_node = {
        # Static properties.
        "object_type": "ExecutableInput",
        "uid": _next_uid().hex(),
        # Fields may have dependencies.
        "data": {"argv": list(argv), "inputs": list(inputs), "stdin": stdin, "environment": dict(**environment)},
    }
    # Results are self-referential until a concrete result is available.
    input_node["result"] = input_node["uid"]

    task_node: dict = {
        # Static properties.
        "object_type": "Executable",
        "uid": _next_uid().hex(),
        "data": {
            "stdout": "stdout.txt",
            "stderr": "stderr.txt",
        },
        # Fields may have dependencies.
        "input": {
            "ExecutableInput": input_node["uid"],
            "resources": dict(**resources),
            # The files produced by a program may depend on the inputs, so this is
            # not static. We will archive the whole working directory, but we can
            # use this file list to do additional checks for task success and allow
            # tighter output binding.
            "output": list(outputs),
        },
    }
    task_node["result"] = task_node["uid"]

    output_node: dict = {
        "object_type": "ExecutableOutput",
        "uid": _next_uid().hex(),
        "input": {
            # Explicit dependency that is not (yet) resolved in terms of data flow.
            "depends": task_node["uid"],
            # Fields with dependencies, though not evident in this example.
            "outputs": list(outputs),
            "stdout": stdout,
            "stderr": stderr,
        },
    }
    output_node["result"] = output_node["uid"]

    return {
        "implementation": ["scalems", "wrappers", "gromacs", "Executable"],
        "message": {"Executable": [input_node, task_node, output_node]},
    }


try:
    from gmxapi.commandline import cli_executable

    _gmx_cli_entry_point = cli_executable()
except ImportError:
    cli_executable = None
    _gmx_cli_entry_point = None
if _gmx_cli_entry_point is None:
    _gmx_cli_entry_point = "gmx"


# TODO: Implicit ensemble handling requires type annotations.
# def make_input(simulation_parameters=['md.mdp'],
#                topology=['md.top'],
#                conformation=['md.gro'],
#                wrapper_name='gmx'):
def make_input(
    simulation_parameters="md.mdp", topology="md.top", conformation="md.gro", wrapper_name=_gmx_cli_entry_point
):
    preprocess = _executable(
        argv=(
            wrapper_name,
            "grompp",
            "-f",
            simulation_parameters,
            "-p",
            topology,
            "-c",
            conformation,
            "-o",
            "topol.tpr",
        )
    )
    return {
        "implementation": ["scalems", "wrappers", "gromacs", "SimulationInput"],
        "message": {"SimulationInput": {"input": preprocess}},
    }


def simulate(arg, **kwargs):
    if not isinstance(arg, dict) or "SimulationInput" not in arg.get("message", ()):
        raise TypeError("Bad input.")
    simulator_input = arg["message"]["SimulationInput"]["input"]
    # Note: We haven't implemented modify_input or simulation chaining yet.
    if not isinstance(simulator_input, dict) or "implementation" not in simulator_input:
        raise TypeError("Wrong kind of input object.")
    return {
        "implementation": ["scalems", "wrappers", "gromacs", "Simulate"],
        "message": {"Simulate": {"input": simulator_input, "kwargs": kwargs}},
    }


def modify_input(*args, **kwargs):
    raise MissingImplementationError()


@functools.singledispatch
def scalems_helper(*args, **kwargs):
    """Allow SCALE-MS to manage tasks from this module.

    Placeholder module entry point. This explicit entry point will probably not
    be required (or appropriate) in the long run. Its roles can be subsumed by
    code that is already run during module import, such as by registration
    through decorators and/or subclassing.

    TODO:
        We can incorporate some amount of Capabilities Provider Interface (CPI)
        here to allow the module author to provide information to the workflow manager
        or dispatch along different run time code paths according to the execution environment.
    """
    logger.debug(
        "scalems_helper called with args:({}), kwargs:({})".format(
            ",".join([repr(arg) for arg in args]),
            ",".join([":".join((key, repr(value))) for key, value in kwargs.items()]),
        )
    )


@scalems_helper.register
def _(task: scalems.workflow.Task, context: WorkflowManager):
    sublogger = logger.getChild("scalems_helper")
    sublogger.debug("Serialized task record: {}".format(task.serialize()))
    command = task.type[-1]
    assert command in {"Simulate", "SimulationInput", "Executable"}
    # Right now, we should only need to process the 'Simulate' command...
    assert command == "Simulate"
    # TODO: Typing on the Task data proxy.
    command_message = task.input["message"][command]
    # kwargs = command_message['kwargs']
    input_ref: str = command_message["input"]
    logger.debug(f"Decoding reference {input_ref}")
    input_ref: bytes = bytes.fromhex(input_ref)
    task_map = context.task_map
    logger.debug(
        "Items done: {}".format(
            ", ".join([": ".join([key.hex(), str(value.done())]) for key, value in task_map.items()])
        )
    )
    # simulator_input_view = context.item(input_ref)

    # TODO: Workaround until we have the framework deliver results.
    # simulator_input: SubprocessResult = simulator_input_view.result()
    # logger.debug(f'Acquired grompp output: {simulator_input}')
    # tprfile = simulator_input.file['-o']
    dependency_dir = pathlib.Path(os.getcwd()).parent / input_ref.hex()
    tprfile = dependency_dir / "topol.tpr"
    import gmxapi

    md_in = gmxapi.read_tpr(str(tprfile))
    # TODO: Manage or integrate with gmxapi working directories.
    md = gmxapi.mdrun(md_in)
    # TODO: Make sure we mark interrupted simulations as "failed".
    return md.run()
