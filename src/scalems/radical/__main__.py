"""Execution dispatched through RADICAL Pilot for ScaleMS workflows.

Usage:
    python3 -m scalems.radical my_workflow.py

"""

import asyncio
import runpy
import sys

import radical.pilot # Make sure rp is imported before logging module.
import scalems.radical

# TODO: Support REPL (e.g. https://github.com/python/cpython/blob/3.8/Lib/asyncio/__main__.py)
if len(sys.argv) < 2:
    raise RuntimeError('Usage: python -m scalems.local myscript.py')

# Note that we want to make sure that the asyncio event loop is started in the
# root thread before any RP Session is created.
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
with scalems.radical.RPWorkflowContext():
    runpy.run_path(sys.argv[0])