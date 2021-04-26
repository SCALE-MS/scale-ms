"""Execution dispatched through RADICAL Pilot for ScaleMS workflows.

Usage:
    python3 -m scalems.radical my_workflow.py

"""

import argparse
import asyncio
import runpy
import sys

import os

import scalems.exceptions
import scalems.radical

# Can we attach to the rp Logger here?

# We can import scalems.context and set module state before using runpy to
# execute the script in the current process. This allows us to preconfigure a
# default execution manager.

# TODO: Consider whether we want launched scripts to have `__name__` set to `__main__` or not.

# TODO: Consider whether we want to parse execution module arguments, including handling chained `-m`.
#     Consider generalizing this boilerplate.


logger = scalems.radical.logger

parser = argparse.ArgumentParser(
    usage='python -m scalems.radical <scalems args> myscript.py <script args>',
    parents=[scalems.radical.parser()]
)


parser.add_argument(
    'script',
    metavar='script-to-run.py',
    type=str,
)

# Strip the current __main__ file from argv. Collect arguments for this script
# and for the called script.
args, script_args = parser.parse_known_args(sys.argv[1:])
if not os.path.exists(args.script):
    # TODO: Support REPL (e.g. https://github.com/python/cpython/blob/3.8/Lib/asyncio/__main__.py)
    raise RuntimeError('Need a script to execute.')

sys.argv = [args.script] + script_args

scalems.radical.configuration(args)

# Start the asyncio event loop on behalf of the client.
# We want to do this exactly once per invocation, and we do not want the scalems
# package module or any particular scalems object to own the event loop.
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


# TODO: Clarify event loop management scheme.
#     Do we want scripts to be like "apps" that get called with asyncio.run(),
#     should we effectively reimplement asyncio.run through scalems.run, or
#     should we think about [ast.PyCF_ALLOW_TOP_LEVEL_AWAIT](https://docs.python.org/3/whatsnew/3.8.html#builtins)

def run_dispatch(work, context: scalems.radical.RPWorkflowContext):
    async def _dispatch(work):
        async with context.dispatch():
            # Add work to the queue
            work()
        # Return an iterable of results.
        # for task in context.tasks: ...
        ...

    loop = context.loop()
    coro = _dispatch(work)
    task = loop.create_task(coro)
    result = loop.run_until_complete(task)
    return result


# Execute the script in the current process.
# TODO: Use Async context by default.
# TODO: More robust dispatching.
# TODO: Can we support mixing invocation with pytest?
exitcode = 0

try:
    with scalems.context.scope(scalems.radical.RPWorkflowContext(loop)) as context:
        try:
            globals_namespace = runpy.run_path(args.script)
            # TODO: Use a decorator to annotate which function(s) to run?
            main = None
            for name, ref in globals_namespace.items():
                if isinstance(ref, scalems.ScriptEntryPoint):
                    if main is not None:
                        raise scalems.exceptions.DispatchError(
                            'Multiple apps in the same script is not (yet?) supported.')
                    main = ref
                    ref.name = name
            if main is None:
                raise scalems.exceptions.DispatchError('No scalems.app callables found in script.')
            # The scalems.run call hierarchy goes through utility.run to utility._run to AsyncWorkflowManager.run,
            # which then goes through WorkflowManager.dispatch(). We should
            # (a) clean this up,
            # (b) return something sensible,
            # (c) clarify error behavior.
            # We probably don't want to be calling scalems.run() from the entry point script.
            # `scalems.run()` and `python -m scalems.backend script.py` are alternative
            # ways to separate the user from the `with Manager.dispatch():` block.
            # For the moment, we can disable the `scalems.run()` mechanism, I think.
            # cmd = scalems.run(main, context=context)

            logger.debug('Starting asyncio run()')
            try:
                result = run_dispatch(main, context)
            except Exception as e:
                logger.exception('Uncaught exception in scalems.run() calling context.run(): ' + str(e))
                raise e
            finally:
                # Note: When we are not using this entry point,
                # we should not assume it is our job to close the event loop,
                # so we do not close the loop at the end of dispatching.
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.stop()
                loop.close()
            assert loop.is_closed()

            logger.debug('Finished asyncio run()')
        except SystemExit as e:
            exitcode = e.code
except Exception as e:
    print('scalems runner encountered Exception: {}'.format(repr(e)))
    if exitcode == 0:
        exitcode = 1
if exitcode != 0:
    raise SystemExit(exitcode)
