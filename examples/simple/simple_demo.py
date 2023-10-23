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
import scalems.simple as simple
from scalems.store import FileStore, FileStoreManager


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
    results_dir = os.path.join(os.getcwd(), 'results_dir')

    work_item0 = simple.WorkItem(
        args=['grompp', input_files, {'-o': 'run.tpr'}], kwargs={}, round_name='round1', step_number=0, tasks_in_step=1)

    manager = FileStoreManager()
    datastore: scalems.store.FileStore = manager.filestore()
    task_list0 = simple.raptor_task_from_work_item(work_item0, 'local')

    #import ipdb;ipdb.set_trace()
    desc = simple.get_pilot_desc(script_config.resource)

    tpr0 = os.path.join('scalems_0_0',f'{task_list0[0].uid}.run.tpr')
    print(f"tpr path: {tpr0}")

    work_item1 = simple.WorkItem(
        args=[['mdrun', '-ntomp', '2'], {'-s': tpr0}, {'-x':'result.xtc', '-c': 'result.gro'}],
        kwargs={}, round_name='round1', step_number=1, tasks_in_step=1)
    task_list1 = simple.raptor_task_from_work_item(work_item1, 'local')
    task_list1[0]['output_staging'] = list()
    import radical.pilot as rp
    task_list1[0]['input_staging'] = {'source': f'client:///{tpr0}',
                                      'target': f'task:///{tpr0}',
                                      'action': rp.TRANSFER}
    #import ipdb;ipdb.set_trace()

    sm = simple.SimpleManager(True)
    sm.prepare_raptor(desc, script_config.venv)
    outs0 = asyncio.run(sm.run_queue(task_list0))
    sm.wait_tasks(outs0)
    outs1 = asyncio.run(sm.run_queue(task_list1))
    sm.wait_tasks(outs1)
    #import ipdb;ipdb.set_trace()

    sm.close()
