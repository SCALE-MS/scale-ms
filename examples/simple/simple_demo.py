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

from scalems.simple import SimpleManager, get_pilot_desc, get_task_info, get_task_path

def run_grompp(input_dir: str, input_gro: str, verbose: bool = False):
    import os
    import gmxapi as gmx
    input_top = os.path.join(input_dir, "topol.top")
    input_mdp = os.path.join(input_dir, "grompp.mdp")
    input_files={'-f': input_mdp, '-p': input_top, '-c': input_gro,},
    tpr = "run.tpr"
    output_files={'-o': tpr}
    grompp = gmx.commandline_operation(gmx.commandline.cli_executable(), 'grompp', input_files, output_files)
    grompp.run()
    if verbose:
        print(grompp.output.stderr.result())
    assert os.path.exists(grompp.output.file['-o'].result())
    return grompp.output.file['-o'].result()

def run_mdrun(tpr_path: str, verbose: bool = False):
    import os
    import gmxapi as gmx
    if not os.path.exists(tpr_path):
        raise FileNotFoundError("You must supply a tpr file")

    input_files={'-s': tpr_path}
    output_files={'-x':'result.xtc', '-c': 'result.gro'}
    md = gmx.commandline_operation(gmx.commandline.cli_executable(), 'mdrun', input_files, output_files)
    md.run()
    if verbose:
        print(md.output.stderr.result())
    assert os.path.exists(md.output.file['-c'].result())
    return md.output.file['-c'].result()

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

    desc = get_pilot_desc(script_config.resource)
    simple_man = SimpleManager()
    simple_man.prepare_raptor(desc, script_config.venv)

    max_cycles=2
    replicates=2
    input_gro = os.path.join(script_config.input_dir, script_config.input_gro)
    prev_step = [input_gro for i in range(replicates)]

    for cycle in range(max_cycles):
        this_step = prev_step
        print(f"step {cycle} has {this_step}")

        # create and submit grompp tasks
        grompp_tasks_list = [simple_man.make_raptor_task(run_grompp(input_dir=script_config.input_dir, input_gro=gro)) for gro in this_step]
        grompp_tasks = simple_man.submit_raptor(grompp_tasks_list)

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




