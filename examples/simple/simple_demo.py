"""
Usage:
python simple_demo.py --venv path/to/scalems/venv/ --resource local.localhost
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from pathlib import Path

import scalems.radical
import radical.pilot as rp
import radical.saga as rs


def get_pilot_desc(resource: str = "local.localhost"):
    description = rp.PilotDescription(
        {
            "resource": resource,
            "runtime": 30,  # pilot runtime minutes
            "exit_on_error": False,
            "project": None,
            "queue": None,
            "cores": 4,
            "gpus": 0,
        }
    )
    return description


class GmxApiRun:
    """Instance of a simulation Command."""

    def __init__(
        self,
        command_line_args,
        input_files,
        output_files,
        venv,
        label: str,
        datastore: scalems.store.FileStore,
    ):
        # TODO: Manage input file staging so we don't have to assume localhost.
        self.args = (
            command_line_args,
            input_files,
            output_files,
        )
        self.label = label
        self._subprocess_call = scalems.call.function_call_to_subprocess(
            func=self._func,
            label=label,
            args=self.args,
            kwargs=None,
            datastore=datastore,
            venv=venv,
        )

    @staticmethod
    def _func(*args):
        """Task implementation."""
        import gmxapi as gmx

        cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), *args)
        return cmd.output.file.result()

    async def result(self, dispatcher: scalems.radical.runtime.RPDispatchingExecutor):
        """Deliver the results of the simulation Command."""
        # Wait for input preparation
        call_handle = await asyncio.create_task(self._subprocess_call)
        rp_task_result_future = asyncio.create_task(
            scalems.radical.task.subprocess_to_rp_task(call_handle, dispatcher=dispatcher)
        )
        # Wait for submission and completion
        rp_task_result = await rp_task_result_future
        result_future = asyncio.create_task(
            scalems.radical.task.wrapped_function_result_from_rp_task(call_handle, rp_task_result)
        )
        # Wait for results staging.
        result: scalems.call.CallResult = await result_future
        # Note that the return_value is the trajectory path in the RP-managed Task directory.
        # TODO: stage trajectory file, explicitly?
        return {"outputs": result.return_value, "task_directory": result.directory}


async def launch(dispatcher, simulations):
    futures = tuple(asyncio.create_task(md.result(dispatcher), name=md.label) for md in simulations)
    for future in futures:
        future.add_done_callback(lambda x: print(f"Task done: {repr(x)}."))
    return await asyncio.gather(*futures)


class DependencySubmitter:
    def __init__(self, dispatcher, grompp_task, mdrun_task):
        self.dispatcher = dispatcher
        self.grompp = asyncio.Event()
        self.mdrun = asyncio.Event()
        self.grompp_task = grompp_task
        self.mdrun_task = mdrun_task

    async def grompp_exec(self):
        print("submitting grompp task")
        grompp_result = await asyncio.create_task(self.grompp_task.result(self.dispatcher), name=self.grompp_task.label)
        self.grompp.set()
        print("grompp task done")
        return grompp_result

    async def mdrun_exec(self):
        print("waiting for grompp to finish")
        await self.grompp.wait()
        print("submitting mdrun task")
        mdrun_result = await asyncio.create_task(self.mdrun_task.result(self.dispatcher), name=self.mdrun_task.label)
        print("mdrun task done")
        return mdrun_result

    async def start(self):
        return await asyncio.gather(self.grompp_exec(), self.mdrun_exec())


async def dep_factory(grompp_task, mdrun_task, dispatcher):
    dep_sub = DependencySubmitter(dispatcher, grompp_task, mdrun_task)
    return await dep_sub.start()


if __name__ == "__main__":
    import gmxapi as gmx

    # Set up a command line argument processor for our script.
    # Inherit from the backend parser so that `parse_known_args` can handle positional arguments the way we want.
    parser = argparse.ArgumentParser(
        parents=[scalems.radical.runtime_configuration.parser()],
        add_help=True,
        description="Parser for simple demo script.",
    )
    parser.add_argument(
        "--input_dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent / "testdata" / "alanine-dipeptide",
        help="Directory containing alanine dipeptide input files. (default: %(default)s)",
    )
    parser.add_argument(
        "--input_gro",
        type=Path,
        default="equil3.gro",
        help="Name of initial gro file to use. (default: %(default)s)",
    )
    # This is necessary to make the script not hang here (???)
    sys.argv.insert(0, __file__)

    script_config, argv = parser.parse_known_args()

    input_files = {
        "-f": os.path.join(script_config.input_dir, "grompp.mdp"),
        "-p": os.path.join(script_config.input_dir, "topol.top"),
        "-c": os.path.join(script_config.input_dir, script_config.input_gro),
    }

    pilot_description = get_pilot_desc(script_config.resource)

    runtime_config = scalems.radical.runtime_configuration.configuration(
        execution_target=pilot_description.resource,
        target_venv=script_config.venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
    )

    loop = asyncio.get_event_loop()
    workflow_manager = scalems.radical.workflow_manager(loop=loop)

    executor = scalems.radical.runtime.executor_factory(workflow_manager, runtime_config)
    executor.rt_startup()

    grompp_run = GmxApiRun(
        command_line_args="grompp",
        input_files=input_files,
        output_files={"-o": "run.tpr"},
        label=f"run-grompp-{0}",
        datastore=workflow_manager.datastore(),
        venv=script_config.venv,
    )

    grompp_tpr = os.path.join(
        rs.Url(executor._runtime_session._pilot.pilot_sandbox).path,
        grompp_run.label,
        "gmxapi.commandline.cli0_i0/run.tpr",
    )

    mdrun_run = GmxApiRun(
        command_line_args=["mdrun", "-ntomp", "2"],
        input_files={"-s": grompp_tpr},
        output_files={"-x": "result.xtc", "-c": "result.gro"},
        label=f"run-mdrun-{0}",
        datastore=workflow_manager.datastore(),
        venv=script_config.venv,
    )

    grompp_result, mdrun_result = asyncio.run(dep_factory(grompp_run, mdrun_run, executor))

    print(grompp_result["outputs"])
    print(mdrun_result["outputs"])

    executor.close()
