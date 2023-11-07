"""
Usage:
python simple_demo.py --venv path/to/scalems/venv/ --resource local.localhost
"""

from __future__ import annotations

import argparse
import h5py
import os
import sys

from pathlib import Path

import scalems.radical
from scalems.simple import SimpleManager, TaskGraph


def mdanalysis(input_files, output_files):
    import MDAnalysis as mda
    import h5py

    u = mda.Universe(input_files["gro"], input_files["xtc"])
    prot = u.select_atoms("protein")
    prot_com = prot.center_of_mass()
    prot_mass = prot.total_mass()

    with h5py.File(output_files["hdf5"], "w") as f:
        f.create_dataset("protein_com", data=prot_com)

    return prot_mass


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

    grompp_task_label = task_graph.add_task_gmxapi(
        command_line_args="grompp", input_files=input_files, output_files={"-o": "run.tpr"}, label=f"run-grompp-{0}"
    )

    mdrun_task_label = task_graph.add_task_gmxapi(
        command_line_args=["mdrun", "-ntomp", "2"],
        input_files={"-s": task_graph.get_task(grompp_task_label).output_files_paths["-o"]},
        output_files={"-x": "result.xtc", "-c": "result.gro"},
        label=f"run-mdrun-{0}",
    )
    task_graph.get_task(mdrun_task_label).add_dependency(grompp_task_label)

    mdanalysis_task_label = task_graph.add_task_python(
        func=mdanalysis,
        input_files={
            "gro": task_graph.get_task(mdrun_task_label).output_files_paths["-c"],
            "xtc": task_graph.get_task(mdrun_task_label).output_files_paths["-x"],
        },
        output_files={"hdf5": "result.hdf5"},
        label=f"run-mdanalysis-{0}",
    )
    task_graph.get_task(mdanalysis_task_label).add_dependency(mdrun_task_label)

    manager.start_session()
    manager.add_task_graph(task_graph.task_graph)
    manager.run_tasks()

    task_graph.plot_graph()
    if task_graph.get_task(mdanalysis_task_label).done():
        with h5py.File(task_graph.get_task(mdanalysis_task_label).output_files_paths["hdf5"], "r") as f:
            print(f"protein com: {f['protein_com'][:]}")
        print(f"protein mass: {task_graph.get_task(mdanalysis_task_label).result():.4f}")

    manager.end_session()
