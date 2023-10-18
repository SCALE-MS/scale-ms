"""
Usage:
python simple_demo.py --venv path/to/scalems/venv/ --resource local.localhost
"""

from __future__ import annotations

import argparse
import os
import sys

from pathlib import Path

import scalems.radical
import asyncio
import radical.pilot as rp

#from scalems.simple import SimpleManager, get_pilot_desc, get_task_info, get_task_path
import scalems.simple as simple

def prepare_gmxapi(args, input_files, output_files):
    import gmxapi as gmx
    cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), args, input_files, output_files)
    return cmd

def run_gmxapi(args, input_files, output_files):
    import gmxapi as gmx
    cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), args, input_files, output_files)
    return cmd.output.file.result()

async def run_gmxapi_async(args, input_files, output_files):
    import gmxapi as gmx
    cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), args, input_files, output_files)
    return cmd.output.file.result()

class GmxApiRun:
    """Instance of a simulation Command."""

    def __init__(
        self,
        command_line_args, input_files, output_files,
        label: str,
        datastore: scalems.store.FileStore,
    ):
        # TODO: Manage input file staging so we don't have to assume localhost.
        self.args = (command_line_args, input_files, output_files,)
        self.label = label
        self.datastore = datastore

    @staticmethod
    def _func(*args):
        """Task implementation."""
        import gmxapi as gmx
        cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), *args)
        return cmd.output.file.result()

    async def result(self, dispatcher: scalems.radical.runtime.RPDispatchingExecutor):
        """Deliver the results of the simulation Command."""
        self._call_handle = scalems.call.function_call_to_subprocess(
            func=self._func,
            label=self.label,
            args=self.args,
            kwargs=None,
            datastore=self.datastore,
        )
        call_handle = await self._call_handle
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
        return {"result": result.return_value, "directory": result.directory}

async def launch(runtime_config: scalems.radical.runtime_configuration.RuntimeConfiguration,
                 simulations):
    async with scalems.execution.dispatch(
        manager, executor_factory=scalems.radical.executor_factory, params=runtime_config
    ) as dispatcher:
        futures = tuple(asyncio.create_task(md.result(dispatcher), name=md.label) for md in simulations)
        for future in futures:
            future.add_done_callback(lambda x: print(f"Task done: {repr(x)}."))
        return await asyncio.gather(*futures)

def get_gmxapi_outputs(gmxapi_futures_list, label: str):
    futures = list()
    for future in gmxapi_futures_list:
        # calling result() has same effect as run() if not already done
        if future.output.file.resource_manager.done():
            futures.append(future.output.file[label].result())
        else:
            futures.append(False)
    return futures

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
    runtime_configuration = scalems.radical.runtime_configuration.configuration(argv)


    input_dir = script_config.input_dir
    input_top = os.path.join(input_dir, "topol.top")
    input_mdp = os.path.join(input_dir, "grompp.mdp")
    input_gro = os.path.join(input_dir, 'equil3.gro')
    input_files={'-f': input_mdp, '-p': input_top, '-c': input_gro,}
    tpr = "run.tpr"
    output_files={'-o': tpr}

    loop=asyncio.get_event_loop()
    manager = scalems.radical.workflow_manager(loop)
    dispatcher=scalems.radical.runtime.executor_factory(manager, runtime_configuration)
    from scalems.radical.executor import _launch_raptor
    #raptor = scalems.radical._launch_raptor(dispatcher)

    grompp = GmxApiRun(command_line_args='grompp',
              input_files=input_files,
              output_files=output_files,
              label=f'grompp-0',
              datastore=manager.datastore(),
              )

    work_item0 = scalems.radical.raptor.ScalemsRaptorWorkItem(
        func=simple.run_gmxapi_radical.__name__, module=simple.run_gmxapi_radical.__module__, args=['grompp', input_files, {'-o': 'run0.tpr'}], kwargs={}, comm_arg_name=None)
    work_item1 = scalems.radical.raptor.ScalemsRaptorWorkItem(
        func=simple.run_gmxapi_radical.__name__, module=simple.run_gmxapi_radical.__module__, args=['grompp', input_files, {'-o': 'run1.tpr'}], kwargs={}, comm_arg_name=None)
    item_task_description = rp.TaskDescription()

    #agrompp=run_gmxapi_async('grompp', input_files, output_files)
    desc = simple.get_pilot_desc(script_config.resource)

    sm = simple.SimpleManager(True)
    sm.prepare_raptor(desc, script_config.venv)
    item_list = [work_item0, work_item1]
    #new_item=simple.run_in_worker(work_item=work_item)
    tasks, outs = asyncio.run(sm.run_queue(item_list))
    import radical.pilot.pytask as pytask
    import ipdb;ipdb.set_trace()
    with scalems.workflow.scope(manager, close_on_exit=True):
        grompp_outputs = asyncio.run(
            launch(
                runtime_config=runtime_configuration,
                simulations=tuple(grompp for i in range(1)),
            ),
            debug=False,
        )


        mdrun_input_files={'-s': grompp_outputs[0]['result']['-o']}
        mdrun_output_files={'-x':'result.xtc', '-c': 'result.gro'}
        mdrun = GmxApiRun(command_line_args='mdrun',
              input_files=mdrun_input_files,
              output_files=mdrun_output_files,
              label=f'mdrun-0',
              datastore=manager.datastore(),
              )
        md_outputs = asyncio.run(
            launch(
                runtime_config=runtime_configuration,
                simulations=tuple(mdrun for i in range(1)),
            ),
            debug=False,
        )
    import ipdb;ipdb.set_trace()
    """


    max_cycles=2
    replicates=2
    input_gro = os.path.join(script_config.input_dir, script_config.input_gro)
    prev_step = [input_gro for i in range(replicates)]

    for cycle in range(max_cycles):
        this_step = prev_step
        print(f"step {cycle} has {this_step}")

        # create and submit grompp tasks
        grompp_tasks_futures = [run_grompp(input_dir=script_config.input_dir, input_gro=gro) for gro in this_step]
        print(f"created futures: {get_gmxapi_outputs(grompp_tasks_futures, '-o')}")
        #import ipdb;ipdb.set_trace()
        grompp_tasks_list = [simple_man.make_raptor_task(task_future) for task_future in grompp_tasks_futures]
        print(f"created tasks: {get_gmxapi_outputs(grompp_tasks_futures, '-o')}")
        grompp_tasks = simple_man.submit_raptor(grompp_tasks_list)
        print(f"run: {get_gmxapi_outputs(grompp_tasks_futures, '-o')}")
        import ipdb;ipdb.set_trace()
        
        # generate an ndx file for later analysis
        ndx_file = "index.ndx"
        select_command = simple_man.make_exe_task(
                executable=f"{gmx.commandline.cli_executable().as_posix()}",
                args_list=["select", "-s", f"{get_task_info(grompp_tasks, 'return_value')[0]}",
                    "-select", "atomnr 5 7 9", "-on", ndx_file],)
        select_task = simple_man.submit_task([select_command])
        ndx_path = os.path.join(get_task_path(select_task[0]), ndx_file)
        assert os.path.exists(ndx_path)

        # create and submit mdrun tasks
        mdrun_tasks_list = [simple_man.make_raptor_task(run_mdrun(tpr_path=tpr)) for tpr in get_task_info(grompp_tasks, 'return_value')]
        mdrun_tasks = simple_man.submit_raptor(mdrun_tasks_list)

        # do some analysis
        angle_tasks_list = [simple_man.make_exe_task(
                    executable=f"{gmx.commandline.cli_executable().as_posix()}",
                    args_list=["angle", "-f", f"{gro}", "-n", f"{ndx_path}"],)
        for gro in get_task_info(mdrun_tasks, 'return_value')]
        angle_task = simple_man.submit_task(angle_tasks_list)
        print(''.join(get_task_info(angle_task, 'stdout')))

        prev_step = get_task_info(mdrun_tasks, 'return_value')
        
    simple_man.close()
    """




