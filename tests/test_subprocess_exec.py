"""Test the scalems.executable command.

TODO: Consider whether we can reimplement as a parameterized test over the
      various executors.
"""

import asyncio
import logging

import pytest

import scalems.context
import scalems.local
import scalems.workflow
from scalems.subprocess import executable


@pytest.mark.asyncio
async def test_executable_local(cleandir):
    # For a similar test dispatching through RADICAL Pilot, see test_rp_exec.py::test_exec_rp
    asyncio.get_event_loop().set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)

    # Test local execution with standard deferred launch.
    # Exercise 1:
    #   * Define the task before entering the dispatch context
    #   * Check the handling for stdin pipelines
    # Note that a coroutine object created from an `async def` function is only awaitable once.
    with scalems.workflow.scope(scalems.local.workflow_manager(asyncio.get_event_loop())) as context:
        # TODO: The `with` block should be equivalent to a `-m scalems.local` invocation. Test.
        # TODO: Input type checking.
        try:
            cmd = executable(("/bin/cat", "-"), stdin=("hi there\n", "hello world"), stdout="stdout.txt")
        except Exception as e:
            # This is just here to give us a place to set a "break" in a debugger.
            raise e
        assert isinstance(cmd, scalems.workflow.ItemView)
        # TODO: Future interface allows client to force resolution of dependencies.
        # cmd.result()
        # TODO(#82): Enable scalems.run()
        # scalems.run(cmd)
        # TODO: Remove Session.run() from public interface (use scalems.run())
        # await context.run()
        async with context.dispatch():
            ...
        result = cmd.result()  # type: scalems.subprocess.SubprocessResult
        assert result.stdout.name == "stdout.txt"
        with open(result.stdout) as fh:
            assert fh.read().startswith("hi there")
    # We need to close the filestore before we begin to initialize a new
    # WorkflowManager so that we don't try to take over an actively managed filestore.
    context.close()

    # Exercise 2:
    #   * Define the task while inside the dispatching context
    with scalems.workflow.scope(scalems.local.workflow_manager(asyncio.get_event_loop())) as context:
        # TODO: Future interface allows client to force resolution of dependencies.
        # cmd.result()
        # TODO(#82): Enable scalems.run()
        # scalems.run(cmd)
        async with context.dispatch():
            # TODO: Input type checking.
            cmd = executable(("/bin/echo", "hello", "world"), stdout="stdout.txt")
            assert isinstance(cmd, scalems.workflow.ItemView)
        result = cmd.result()  # type: scalems.subprocess.SubprocessResult
        assert result.stdout.name == "stdout.txt"
        with open(result.stdout) as fh:
            assert fh.read().startswith("hello world")
    # We need to close the filestore before removing the temporary directory to avoid
    # errors.
    context.close()
