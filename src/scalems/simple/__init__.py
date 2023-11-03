from __future__ import annotations

import asyncio
import os

from dataclasses import dataclass
from typing import Union
import networkx as nx
import matplotlib.pyplot as plt

import scalems.radical
import radical.pilot as rp


class SimpleManager:
    def __init__(self, script_config):
        """
        Initializes manager and session executor but not session.
        This separation is enforced so that workflows can be built before session startup.
        """
        pilot_description = get_pilot_desc(script_config.resource)

        self.venv = script_config.venv
        runtime_config = scalems.radical.runtime_configuration.configuration(
            execution_target=pilot_description.resource,
            target_venv=self.venv,
            rp_resource_params={"PilotDescription": pilot_description.as_dict()},
        )

        loop = asyncio.get_event_loop()
        self._workflow_manager = scalems.radical.workflow_manager(loop=loop)

        self._executor = scalems.radical.runtime.executor_factory(self._workflow_manager, runtime_config)

        self.task_list = dict()

    def start_session(self):
        self._executor.rt_startup()

    def end_session(self):
        self._executor.close()

    def submit(self, command_line_args, input_files, output_files, label: str):
        call = scalems.call.gmxapi_function_call_to_subprocess(
            func=gmxapi_call,
            label=label,
            command_line_args=command_line_args,
            input_files=input_files,
            output_files=output_files,
            datastore=self._executor.datastore,
            venv=self.venv,
        )
        task = GmxApiTask(
            command_line_args=command_line_args,
            input_files=input_files,
            output_files=output_files,
            label=label,
            output_dir=self._executor.datastore.datastore.as_posix(),
        )
        managed = ManagedTask(call, label, task)
        self.task_list[task.label] = managed
        return task

    def plot_graph(self):
        graph = nx.DiGraph()
        for task_label in self.task_list:
            graph.add_node(task_label)
        for task_label in self.task_list:
            for dependency in self.task_list[task_label].gmxapi.dependencies():
                graph.add_edge(dependency, task_label)

        nx.draw(graph, with_labels=True)
        plt.show()

    def run_tasks(self):
        asyncio.run(self._start())

    def _create_events(self):
        for task_label in self.task_list:
            self.task_list[task_label].event = asyncio.Event()

    async def _exec(self, task_label: str):
        task = self.task_list[task_label]
        if task.gmxapi.dependencies:
            for dependency in task.gmxapi.dependencies():
                print(f"waiting for {dependency} before running {task_label}")
                await self.task_list[dependency].event.wait()
        print(f"submitting {task_label} task")
        result = await asyncio.create_task(gmxapi_result(task.call, self._executor), name=task_label)
        task.event.set()
        print(f"task {task_label} done")
        return result

    async def _start(self):
        self._create_events()
        results =  await asyncio.gather(*[self._exec(task_label) for task_label in self.task_list])
        for result in results:
            task_label = result.label
            self.task_list[task_label].gmxapi._result = result

    def executable(self):
        """Wrapper to make executable for submission."""
        pass


def gmxapi_call(*args):
    """Task implementation."""
    import gmxapi as gmx

    cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), *args)
    return cmd.output.file.result()

@dataclass
class ManagedTask:
    call: scalems.call.GmxApiSubprocess
    label: str
    gmxapi: GmxApiTask
    event: Union[asyncio.Event, None] = None


class GmxApiTask:
    def __init__(
        self,
        command_line_args,
        input_files,
        output_files,
        output_dir,
        label: str,
    ):
        self.args = (
            command_line_args,
            input_files,
            output_files,
        )
        self.label = label

        self._output_files = {
            flag: os.path.join(output_dir, f"{label}.{name}") for flag, name in output_files.items()
        }
        self._dependencies = []
        self._result = None

    def output_files(self):
        return self._output_files

    def add_dependency(self, task_label: str):
        self._dependencies.append(task_label)

    def dependencies(self):
        return self._dependencies

    def status(self):
        """Returns Pending / Complete / Failed or similar, ideally per ensemble member."""
        if self._result is None:
            return "Pending"
        elif self._result.exception is not None:
            return self._result.exception
        elif self._result.return_value is not None:
            return "Complete"
        else:
            return "Unknown"

    def outputs(self):
        """Returns output variables and handles to files."""
        if self.status() == "Complete":
            return self._output_files
        else:
            return self.status()


async def gmxapi_result(subprocess_call, dispatcher: scalems.radical.runtime.RPDispatchingExecutor):
    """Deliver the results of the simulation Command."""
    # Wait for input preparation
    call_handle = await asyncio.create_task(subprocess_call)
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
    return result


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
