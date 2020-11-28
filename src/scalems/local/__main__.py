"""Local execution dispatching for ScaleMS workflows.

Usage:
    python3 -m scalems.local my_workflow.py

"""

import asyncio
import runpy
import sys

import scalems.local

# We can import scalems.context and set module state before using runpy to
# execute the script in the current process. This allows us to preconfigure a
# default execution manager.

# TODO: Consider whether we want launched scripts to have `__name__` set to `__main__` or not.

# TODO: Consider whether we want to parse execution module arguments, including handling chained `-m`.
#     Consider generalizing this boilerplate.

# TODO: Support REPL (e.g. https://github.com/python/cpython/blob/3.8/Lib/asyncio/__main__.py)
if len(sys.argv) < 2:
    raise RuntimeError('Usage: python -m scalems.local myscript.py')

# Strip the current __main__ file from argv
sys.argv[:] = sys.argv[1:]

# Start the asyncio event loop on behalf of the client.
# We want to do this exactly once per invocation, and we do not want the scalems
# package module or any particular scalems object to own the event loop.
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
# TODO: Clarify event loop management scheme.
#     Do we want scripts to be like "apps" that get called with asyncio.run(),
#     should we effectively reimplement asyncio.run through scalems.run, or
#     should we think about [ast.PyCF_ALLOW_TOP_LEVEL_AWAIT](https://docs.python.org/3/whatsnew/3.8.html#builtins)

# Execute the script in the current process.
# TODO: Use Async context by default.
# TODO: More robust dispatching.
# TODO: Can we support mixing invocation with pytest?
exitcode = 0
try:
    with scalems.context.scope(scalems.local.AsyncWorkflowManager()) as context:
        try:
            # Check accessibility!
            globals_namespace = runpy.run_path(sys.argv[0])
            # TODO: Use a decorator to annotate which function(s) to run?
            main = None
            for name, ref in globals_namespace.items():
                if isinstance(ref, scalems.core.ScriptEntryPoint):
                    if main is not None:
                        raise scalems.core.exceptions.DispatchError('Multiple apps in the same script is not (yet?) supported.')
                    main = ref
                    ref.name = name
            if main is None:
                raise scalems.core.exceptions.DispatchError('No scalems.app callables found in script.')
            cmd = scalems.run(main, context=context)
        except SystemExit as e:
            exitcode = e.code
except Exception as e:
    print('Exception: {}'.format(repr(e)))
    if exitcode == 0:
        exitcode = 1
raise SystemExit(exitcode)