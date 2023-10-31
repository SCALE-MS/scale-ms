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
from dataclasses import dataclass
from typing import Union

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


@dataclass
class Task:
    task: GmxApiRun
    dependencies: list[str]
    event: Union[asyncio.Event, None] = None


class DependencySubmitter:
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher
        self.task_list = dict()

    def add_task(self, task: Task):
        self.task_list[task.task.label] = task

    def _create_events(self):
        for task_label in self.task_list:
            self.task_list[task_label].event = asyncio.Event()

    async def exec(self, task_label: str):
        task = self.task_list[task_label]
        if task.dependencies:
            for dependency in task.dependencies:
                print(f"waiting for {dependency} before running {task_label}")
                await self.task_list[dependency].event.wait()
        print(f"submitting {task_label} task")
        result = await asyncio.create_task(task.task.result(self.dispatcher), name=task_label)
        task.event.set()
        print(f"task {task_label} done")
        return result

    async def start(self):
        self._create_events()
        return await asyncio.gather(*[self.exec(task_label) for task_label in self.task_list])


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

    dep_sub = DependencySubmitter(executor)
    dep_sub.add_task(Task(grompp_run, []))
    dep_sub.add_task(Task(mdrun_run, [grompp_run.label]))
    grompp_result, mdrun_result = asyncio.run(dep_sub.start())

    print(grompp_result["outputs"])
    print(mdrun_result["outputs"])

    executor.close()
