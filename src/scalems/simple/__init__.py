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
        if self._executor.runtime is not None:
            self._executor.close()

    def add_task_graph(self, task_graph: dict):
        gmxapi_sub = partial(
            scalems.call.simple_function_call_to_subprocess,
            datastore=self._executor.datastore,
            venv=self.venv,
        )
        for task_label in task_graph:
            task = task_graph[task_label]
            if task.done():
                self.task_list[task_label] = FinishedTask(task_label)
                continue

            call = gmxapi_sub(
                func=task._func,
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
        if isinstance(task, FinishedTask):
            pass
        else:
            if task.task.dependencies:
                for dependency in task.task.dependencies():
                    if isinstance(self.task_list[dependency], FinishedTask):
                        print(f"dependency {dependency} for {task_label} already complete")
                        continue
                    else:
                        print(f"waiting for {dependency} before running {task_label}")
                        await self.task_list[dependency].event.wait()
            print(f"submitting {task_label} task")
            result = await asyncio.create_task(
                gmxapi_result(task.call, self._executor, task.task._pad_string), name=task_label
            )
            task.event.set()
            print(f"task {task_label} done")
            self.task_list[task_label].task._result = result
            self._executor.datastore.add_data_to_task(task_label, "_result", result)
            self._executor.datastore.flush()

    async def _start(self):
        self._create_events()
        await asyncio.gather(*[self._exec(task_label) for task_label in self.task_list])

    def executable(self):
        """Wrapper to make executable for submission."""
        pass


class TaskGraph:
    def __init__(self, manager: SimpleManager):
        self.datastore = manager._executor.datastore
        self.task_graph = dict()

    def _check_task_status(self, label: str, task_type: str = "gmxapi"):
        fs = self.datastore
        task_in_fs = fs.get_task(label)
        if task_in_fs:
            task_in_fs = task_in_fs["task"]
            if task_type == "gmxapi":
                task_in_fs = GmxApiTask.from_dict(task_in_fs)
            elif task_type == "python":
                task_in_fs = PythonTask.from_dict(task_in_fs)
            else:
                raise ValueError(f"task_type must be 'gmxapi' or 'python', not {task_type}")
            if task_in_fs.done():
                print(f"task {label} already complete")
                self.task_graph[label] = task_in_fs
                return task_in_fs.label
            else:
                if all(os.path.isfile(fn) for fn in task_in_fs.output_files_paths.values()):
                    print(f"all output files exist, but result not properly recorded; running again to be safe")
                    # Is there a good way to recover from this state? Then we can do the following:
                    # self.task_graph[label] = task_in_fs
                    # return task_in_fs.label
                    fs.remove_task(label)
                    fs.flush()
                else:
                    print(f"some output files missing, rerunning")

    def _write_task_to_fs(self, task: Union[GmxApiTask, PythonTask]):
        # We should only get here when the task still needs to be run
        # Consider adding something like the following:
        # assert task_in_fs is None or task_in_fs.result() != "Complete"
        task_metadata = {"task": task.__dict__}
        fs = self.datastore
        fs.add_task(task.label, **task_metadata)
        fs.flush()

    def add_task_gmxapi(self, command_line_args, input_files, output_files, label: str):
        check = self._check_task_status(label)
        if check:
            return check
        task = GmxApiTask(
            command_line_args=command_line_args,
            input_files=input_files,
            output_files=output_files,
            label=label,
            output_dir=self.datastore.datastore.as_posix(),
        )
        self._write_task_to_fs(task)
        self.task_graph[task.label] = task
        return task.label

    def add_task_python(self, func, input_files, output_files, label: str):
        check = self._check_task_status(label)
        if check:
            return check
        task = PythonTask(
            python_func=func,
            input_files=input_files,
            output_files=output_files,
            label=label,
            output_dir=self.datastore.datastore.as_posix(),
        )
        self._write_task_to_fs(task)
        self.task_graph[task.label] = task
        return task.label

    def get_task(self, label: str):
        return self.task_graph[label]

    def plot_graph(self):
        graph = nx.DiGraph()
        for task_label in self.task_graph:
            if self.task_graph[task_label].done():
                graph.add_node(task_label, color="green")
            else:
                graph.add_node(task_label, color="red")

        for task_label in self.task_graph:
            for dependency in self.task_graph[task_label].dependencies():
                graph.add_edge(dependency, task_label)

        colors = [node[1]["color"] for node in graph.nodes(data=True)]
        nx.draw(graph, node_color=colors, with_labels=True)
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
    task: Union[GmxApiTask, PythonTask]
    event: Union[asyncio.Event, None] = None


@dataclass
class FinishedTask:
    label: str


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
        self._func = scalems.call.to_bytes(gmxapi_call).hex()
        self._pad_string = "gmxapi.commandline.cli0_i0/"

    @property
    def output_files_paths(self):
        return self.__output_files_paths

    @output_files_paths.setter
    def output_files_paths(self, output_files):
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

    @classmethod
    def from_dict(cls, dict_):
        new_class = cls(
            dict_["command_line_args"],
            dict_["input_files"],
            dict_["output_files"],
            dict_["_output_dir"],
            dict_["label"],
        )
        if dict_["_func"]:
            new_class._func = dict_["_func"]
        if dict_["_dependencies"]:
            new_class._dependencies = dict_["_dependencies"]
        if dict_["_result"]:
            new_class._result = scalems.call.CallResult(**dict_["_result"])
        if dict_["_pad_string"]:
            new_class._pad_string = dict_["_pad_string"]
        return new_class


class PythonTask(GmxApiTask):
    def __init__(
        self,
        python_func,
        input_files,
        output_files,
        output_dir,
        label: str,
    ):
        super().__init__(None, input_files, output_files, output_dir, label)
        self._func = scalems.call.to_bytes(python_func).hex()
        self._pad_string = ""

    @classmethod
    def from_dict(cls, dict_):
        new_class = cls(
            dict_["_func"],
            dict_["input_files"],
            dict_["output_files"],
            dict_["_output_dir"],
            dict_["label"],
        )
        if dict_["_dependencies"]:
            new_class._dependencies = dict_["_dependencies"]
        if dict_["_result"]:
            new_class._result = scalems.call.CallResult(**dict_["_result"])
        if dict_["_pad_string"]:
            new_class._pad_string = dict_["_pad_string"]
        return new_class


async def gmxapi_result(subprocess_call, dispatcher: scalems.radical.runtime.RPDispatchingExecutor, pad_string=""):
    """Deliver the results of the simulation Command."""
    # Wait for input preparation
    call_handle = await asyncio.create_task(subprocess_call)
    rp_task_result_future = asyncio.create_task(
        scalems.radical.task.subprocess_to_rp_task(call_handle, dispatcher=dispatcher, pad_string=pad_string)
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
