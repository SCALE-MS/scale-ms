from __future__ import annotations

import asyncio
import os

from dataclasses import dataclass
from functools import partial
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
        if self._executor.runtime is None:
            self._executor.rt_startup()

    def end_session(self):
        self._executor.close()

    def add_task_graph(self, task_graph: dict):
        gmxapi_sub = partial(scalems.call.gmxapi_function_call_to_subprocess,
                             func=gmxapi_call, datastore=self._executor.datastore, venv=self.venv)
        for task_label in task_graph:
            task = task_graph[task_label]
            call = gmxapi_sub(
                label=task_label,
                command_line_args=task.command_line_args,
                input_files=task.input_files,
                output_files=task.output_files,
            )
            managed = ManagedTask(call, task_label, task)
            self.task_list[task.label] = managed

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

class TaskGraph:
    def __init__(self, manager: SimpleManager):
        self.datastore = manager._executor.datastore.datastore
        self.task_graph = dict()

    def add_task(self, command_line_args, input_files, output_files, label: str):
        task = GmxApiTask(
            command_line_args=command_line_args,
            input_files=input_files,
            output_files=output_files,
            label=label,
            output_dir=self.datastore.as_posix(),
        )
        self.task_graph[task.label] = task
        return task

    def plot_graph(self):
        graph = nx.DiGraph()
        for task_label in self.task_graph:
            graph.add_node(task_label)
        for task_label in self.task_graph:
            for dependency in self.task_graph[task_label].dependencies():
                graph.add_edge(dependency, task_label)

        nx.draw(graph, with_labels=True)
        plt.show()


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
        self.command_line_args = command_line_args
        self.input_files = input_files
        self.output_files = output_files
        self._output_dir = output_dir
        self.label = label

        self.output_files_paths = output_files


        self._dependencies = []
        self._result = None

    @property
    def output_files_paths(self):
        return self.__output_files_paths

    @output_files_paths.setter
    def output_files_paths(self, output_files):
        print("setting output files paths")
        self.__output_files_paths = {
            flag: os.path.join(self._output_dir, f"{self.label}.{name}") for flag, name in output_files.items()
        }

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

    def done(self) -> bool:
        """Returns True if the task is complete."""
        return self.status() == "Complete"

    def result(self):
        """Returns output variables and handles to files."""
        if self.done():
            return self._result.return_value
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
