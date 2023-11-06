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
from scalems.simple import SimpleManager, TaskGraph


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

    manager = SimpleManager(script_config)
    task_graph = TaskGraph(manager)
    manager.start_session()

    grompp_task_label = task_graph.add_task(
        command_line_args="grompp", input_files=input_files, output_files={"-o": "run.tpr"}, label=f"run-grompp-{0}"
    )

    mdrun_task_label = task_graph.add_task(
        command_line_args=["mdrun", "-ntomp", "2"],
        input_files={"-s": task_graph.get_task(grompp_task_label).output_files_paths["-o"]},
        output_files={"-x": "result.xtc", "-c": "result.gro"},
        label=f"run-mdrun-{0}",
    )
    task_graph.get_task(mdrun_task_label).add_dependency(grompp_task_label)

    manager.add_task_graph(task_graph.task_graph)

    manager.run_tasks()

    if task_graph.get_task(mdrun_task_label).done():
        print(f" Outputs from mdrun:\n{task_graph.get_task(mdrun_task_label).result()}")
        assert os.path.isfile(task_graph.get_task(mdrun_task_label).output_files_paths["-x"])

    manager.end_session()
