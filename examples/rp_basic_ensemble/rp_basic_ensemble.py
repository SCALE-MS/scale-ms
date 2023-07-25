"""Prepare and run an ensemble of trajectories through RADICAL Pilot.

We will rely on RADICAL Pilot to manage resource allocation. For simplicity,
this example assumes that the local.localhost RP resource is configured appropriately,
and that the "local" access scheme is sufficient.

This example is tested with thread-MPI GROMACS. MPI parallelism in scalems.radical
tasks is under investigation.

The *cores-per-sim* must not exceed the cores available to the pilot.

For example, for an ensemble of size 2, and 2 cores per simulation,
activate your gmxapi Python virtual environment.

For thread-MPI gromacs, run::

    python rp_basic_ensemble.py \
        --resource local.localhost \
        --access ssh \
        --venv $VIRTUAL_ENV \
        --pilot-option cores=4 \
        --procs-per-sim 1 \
        --threads-per-sim 2 \
        --mdrun-arg nt 2 \
        --mdrun-arg ntomp 1 \
        --size 2 \
        --mdrun-arg maxh 1.0

or::

    python rp_basic_ensemble.py \
        --resource local.localhost \
        --access ssh \
        --venv $VIRTUAL_ENV \
        --pilot-option cores=4 \
        --procs-per-sim 1 \
        --threads-per-sim 2 \
        --mdrun-arg nt 2 \
        --size 2 \
        --mdrun-arg maxh 1.0

For MPI GROMACS::

    python rp_basic_ensemble.py \
        --resource local.localhost \
        --access ssh \
        --venv $VIRTUAL_ENV \
        --pilot-option cores=4 \
        --procs-per-sim 2 \
        --size 2 \
        --mdrun-arg maxh 1.0

Note that RP access schemes based on forking the Python interpreter may not work
right with MPI-enabled tasks. Check the resource definition and prefer an access
scheme that uses ``ssh`` or a job management system, like ``slurm``.

"""
from __future__ import annotations

import asyncio
import argparse
import logging
import os
import sys
import urllib.parse
from pathlib import Path

import scalems
import scalems.call
import scalems.execution
import scalems.radical
import scalems.radical.task
import scalems.workflow
from _simulation import executable
from _simulation import MDRun
from _simulation import MDRunResult
from _simulation import output_file


async def main(
    *,
    script_config: argparse.Namespace,
    runtime_config: scalems.radical.runtime_configuration.RuntimeConfiguration,
    manager: scalems.workflow.WorkflowManager,
    label: str,
) -> tuple[MDRunResult]:
    """Gromacs simulation on ensemble input

    Call the GROMACS MD preprocessor to create a simulation input file. Declare an
    ensemble simulation workflow starting from the single input file.

    Args:
        runtime_config: runtime configuration for scalems.radical backend.
        script_config: namespace or named tuple with configuration values. (Refer to the argparse parser.)
        manager: an active scalems WorkflowManager instance.

    Returns:
        Trajectory output. (list, if ensemble simulation)
    """
    import gmxapi as gmx

    input_dir: Path = script_config.inputs
    ensemble_size: int = script_config.size
    mdrun_args = {opt[0]: " ".join(opt[1:]) for opt in script_config.mdrun_args}

    commandline = [
        gmx.commandline.cli_executable(),
        "pdb2gmx",
        "-ff",
        "amber99sb-ildn",
        "-water",
        "tip3p",
        "-f",
        os.path.join(input_dir, "start0.pdb"),
        "-p",
        output_file("topol.top"),
        "-i",
        output_file("posre.itp"),
        "-o",
        output_file("conf.gro"),
    ]
    make_top = executable(commandline)

    # make array of inputs
    commandline = [
        gmx.commandline.cli_executable(),
        "grompp",
        "-f",
        os.path.join(input_dir, "grompp.mdp"),
        # TODO: executable task output proxy
        # '-c', make_top.output_file['conf.gro'],
        # '-p', make_top.output_file['topol.top'],
        "-c",
        make_top.output.file["-o"],
        "-p",
        make_top.output.file["-p"],
        "-o",
        output_file("run.tpr", label="simulation_input"),
    ]
    grompp = executable([commandline])

    # TODO: executable task output proxy
    # tpr_input = grompp.output_file['simulation_input']
    tpr_input = grompp.output.file["-o"].result()

    session: scalems.radical.runtime.RPDispatchingExecutor
    async with scalems.execution.dispatch(
        manager, executor_factory=scalems.radical.executor_factory, params=runtime_config
    ) as dispatcher:
        simulations = tuple(
            MDRun(
                tpr_input,
                runtime_args=mdrun_args,
                task_ranks=script_config.procs_per_sim,
                task_cores_per_rank=script_config.threads_per_sim,
                manager=manager,
                dispatcher=dispatcher,
                label=f"{label}-{i}",
            )
            for i in range(ensemble_size)
        )
        futures = tuple(asyncio.create_task(md.result(), name=md.label) for md in simulations)
        for future in futures:
            future.add_done_callback(lambda x: print(f"Task done: {repr(x)}."))
        return await asyncio.gather(*futures)


if __name__ == "__main__":
    import dill
    import scalems.radical

    dill.settings["recurse"] = True

    # Set up a command line argument processor for our script.
    # Inherit from the backend parser so that `parse_known_args` can handle positional arguments the way we want.
    parser = argparse.ArgumentParser(
        parents=[scalems.radical.runtime_configuration.parser()],
        add_help=True,
        description="Warning: The automatically generated usage information is not quite right, "
        "pending normalization with the scalems backend invocation. "
        "Refer to the docstring in the file for details.",
    )

    parser.add_argument(
        "--procs-per-sim",
        type=int,
        default=1,
        help="Processes (MPI ranks) per simulation task. (default: %(default)s)",
    )
    parser.add_argument(
        "--threads-per-sim",
        type=int,
        default=1,
        help="OMP_NUM_THREADS in simulation processes. (default: %(default)s)",
    )
    # I wasn't able to quickly find a user-friendly way to process arbitrarily multi-valued
    # arguments that, themselves, look like arguments that would terminate a `nargs="*"` stream.
    parser.add_argument(
        "--mdrun-arg",
        dest="mdrun_args",
        action="append",
        default=[],
        nargs="*",
        help="Option flag (with the leading '-' removed)"
        " and value(s) to be passed to the GROMACS simulator. Use once per option.",
        metavar="OPTION VAL1 [VAL2]",
    )
    parser.add_argument(
        "--inputs",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent / "testdata" / "fs-peptide",
        help="Directory containing fs-peptide input files. (default: %(default)s)",
    )
    # parser.add_argument('--log-level', default='ERROR',
    #     help='Minimum log level to handle for the Python "logging" module. (See '
    #          'https://docs.python.org/3/library/logging.html#logging-levels)')
    parser.add_argument(
        "--size",
        type=int,
        default=1,
        help="Ensemble size: number of parallel pipelines. (default: %(default)s)",
    )

    # Work around some quirks: we are using the parser that normally assumes the
    # backend from the command line. We can switch back to the `-m scalems.radical`
    # style invocation when we have some more updated UI tools
    # (e.g. when scalems.wait has been updated) and `main` doesn't have to be a coroutine.
    sys.argv.insert(0, __file__)

    # Handle command line invocation.
    script_config, argv = parser.parse_known_args()
    runtime_configuration = scalems.radical.runtime_configuration.configuration(argv)

    # Configure logging module before using tools that use it.
    level = None
    debug = False
    if script_config.log_level is not None:
        level = logging.getLevelName(script_config.log_level.upper())
        debug = level <= logging.DEBUG
    if level is not None:
        character_stream = logging.StreamHandler()
        character_stream.setLevel(level)
        formatter = logging.Formatter("%(asctime)s-%(name)s:%(lineno)d-%(levelname)s - %(message)s")
        character_stream.setFormatter(formatter)
        logging.basicConfig(level=level, handlers=[character_stream])

    logging.info(f"Input directory set to {script_config.inputs}.")

    # Call the main work.
    manager = scalems.radical.workflow_manager(asyncio.get_event_loop())
    with scalems.workflow.scope(manager, close_on_exit=True):
        md_outputs = asyncio.run(
            main(
                script_config=script_config,
                runtime_config=runtime_configuration,
                manager=manager,
                label="rp-basic-ensemble",
            ),
            debug=debug,
        )

    trajectory = [md["trajectory"] for md in md_outputs]
    directory = [md["directory"] for md in md_outputs]

    # Write output file manifests
    with open("zipfiles.manifest", "w") as fh:
        for url in directory:
            # These paths are in the "local" filestore (`./scalems_X_Y/`)
            fh.write(urllib.parse.urlparse(url).path + "\n")
    with open("trajectories.manifest", "w") as fh:
        for path in trajectory:
            # These paths are in the execution environment.
            fh.write(str(path) + "\n")

    for i, out in enumerate(zip(trajectory, directory)):
        print(f"Trajectory {i}: {out[0]}. Directory archive (zip file) {i}: {out[1]}")
