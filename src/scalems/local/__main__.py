"""Local execution dispatching for ScaleMS workflows.

Usage:
    python3 -m scalems.local my_workflow.py

"""

import runpy
import sys

import scalems.local

# We can import scalems.context and set module state before using runpy to
# execute the script in the current process. This allows us to preconfigure a
# default execution manager.

# TODO: Consider whether we want launched scripts to have `__name__` set to `__main__` or not.

# TODO: Consider whether we want to parse execution module arguments, including handling chained `-m`.
#     Consider generalizing this boilerplate.

# Strip the current __main__ file from argv
sys.argv[:] = sys.argv[1:]
# Execute the script in the current process.
# TODO: Use Async context by default.
# TODO: More robust dispatching.
# TODO: Can we support mixing invocation with pytest?
with scalems.local.ImmediateExecutionContext():
    runpy.run_path(sys.argv[0])
