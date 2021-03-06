"""Provide the boiler plate for scalems entry point scripts.

Internal module provides consistent logic for command line invocation.

Supports invocation of the following form with minimal `backend/__main__.py`

    python -m scalems.radical --venv=/path/to/venv --resource=local.localhost myscript.py arg1 --foo bar

"""

import argparse
import asyncio
import os
import runpy
import sys
import threading
import typing

import scalems.exceptions
import scalems.workflow
from scalems.utility import parser as base_parser

_reentrance_guard = threading.Lock()


# We can import scalems.context and set module state before using runpy to
# execute the script in the current process. This allows us to preconfigure a
# default execution manager.

# TODO: Consider whether we want launched scripts to have `__name__` set to `__main__` or not.

# TODO: Consider whether we want to parse execution module arguments, including handling chained `-m`.
#     Consider generalizing this boilerplate.

# TODO: Support REPL (e.g. https://github.com/python/cpython/blob/3.8/Lib/asyncio/__main__.py)


def run_dispatch(work, context: scalems.workflow.WorkflowManager):
    async def _dispatch(_work):
        async with context.dispatch():
            # Add work to the queue
            _work()
        # Return an iterable of results.
        # for task in context.tasks: ...
        ...

    _loop = context.loop()
    _coro = _dispatch(work)
    _task = _loop.create_task(_coro)
    _result = _loop.run_until_complete(_task)
    return _result


def run(manager_type: typing.Type[scalems.workflow.WorkflowManager],  # noqa: C901
        _loop: asyncio.AbstractEventLoop = None):
    safe = _reentrance_guard.acquire(blocking=False)
    if not safe:
        raise RuntimeError('scalems launcher is not reentrant.')
    try:
        module = sys.modules[manager_type.__module__]

        get_parser = getattr(module, 'parser', base_parser)

        parser = argparse.ArgumentParser(
            usage=f'python -m {module.__name__} <{module.__name__} args> myscript.py <script args>',
            parents=[get_parser()]
        )

        parser.add_argument(
            'script',
            metavar='script-to-run.py',
            type=str,
        )

        # Strip the current __main__ file from argv. Collect arguments for this script
        # and for the called script.
        args, script_args = parser.parse_known_args(sys.argv[1:])

        if args.pycharm:
            try:
                import pydevd_pycharm
                pydevd_pycharm.settrace('host.docker.internal', port=12345, stdoutToServer=True, stderrToServer=True)
            except ImportError:
                ...

        if not os.path.exists(args.script):
            # TODO: Support REPL (e.g. https://github.com/python/cpython/blob/3.8/Lib/asyncio/__main__.py)
            raise RuntimeError('Need a script to execute.')

        level = args.log_level
        if level is not None:
            import logging
            character_stream = logging.StreamHandler()
            # Optional: Set log level.
            logging.getLogger('scalems').setLevel(level)
            character_stream.setLevel(level)
            # Optional: create formatter and add to character stream handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            character_stream.setFormatter(formatter)
            # add handler to logger
            logging.getLogger('scalems').addHandler(character_stream)
        logger = getattr(module, 'logger')

        configure_module = getattr(module, '_set_configuration', None)
        if configure_module is not None:
            configure_module(args)
        if hasattr(module, 'configuration'):
            config = module.configuration()
            logger.debug(f'Configuration: {config}')

        sys.argv = [args.script] + script_args

        if _loop is not None:
            # Allow event loop to be provided (for debugging and testing purposes).
            loop = _loop
        else:
            # Start the asyncio event loop on behalf of the client.
            # We want to do this exactly once per invocation, and we do not want the scalems
            # package module or any particular scalems object to own the event loop.
            loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # TODO: Clarify event loop management scheme.
        #     Do we want scripts to be like "apps" that get called with asyncio.run(),
        #     should we effectively reimplement asyncio.run through scalems.run, or
        #     should we think about
        #     [ast.PyCF_ALLOW_TOP_LEVEL_AWAIT](https://docs.python.org/3/whatsnew/3.8.html#builtins)

        # Execute the script in the current process.
        # TODO: Can we support mixing invocation with pytest?
        exitcode = 0

        try:
            with scalems.context.scope(manager_type(loop)) as manager:
                try:
                    globals_namespace = runpy.run_path(args.script)

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
                    # The scalems.run call hierarchy goes through utility.run
                    # to utility._run to AsyncWorkflowManager.run,
                    # which then goes through WorkflowManager.dispatch().
                    # We should
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
                        run_dispatch(main, manager)
                    except Exception as e:
                        logger.exception('Uncaught exception in scalems runner calling dispatch(): ' + str(e))
                        raise e
                    finally:
                        loop.close()
                    assert loop.is_closed()

                    logger.debug('Finished asyncio run()')
                except SystemExit as e:
                    exitcode = e.code
        except Exception as e:
            print(f'{module} encountered Exception: {repr(e)}')
            if exitcode == 0:
                exitcode = 1
        if exitcode != 0:
            raise SystemExit(exitcode)
    finally:
        _reentrance_guard.release()
