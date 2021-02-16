"""Test the scalems.executable command.

TODO: Consider whether we can reimplement as a parameterized test over the
      various executors.
"""

import asyncio
import logging

import pytest

import scalems.context
import scalems.local
from scalems.core.exceptions import MissingImplementationError
import scalems.local_immediate
from scalems.subprocess import executable


def test_exec_default():
    # Check for expected behavior of the default context
    with pytest.raises(MissingImplementationError):
        # Test default context
        cmd = executable(('/bin/echo',))
        context = scalems.context.get_context()
        # Note: we can choose to let Context.run(...) return a contextmanager that
        # can be used to create a session (if not already entered) that cleans up
        # after itself as best it can through a weakref from the context when the
        # reference count goes to zero, making the `with` block optional, but it
        # may be better to avoid too much automation or alternative ways to do things.
        with context as session:
            session.run(cmd)


def test_exec_immediate():
    # Test immediate execution.
    context = scalems.local_immediate.ImmediateExecutionContext()
    with scalems.context.scope(context) as session:
        # TODO: Automatic context subscription so `cmd` can be obtained before non-default context.
        # Cross-context dispatching with the root context is not an important core feature now,
        # but it is a trivial test case for important cross-context dispatching in the near term,
        # and would be necessary for automated dispatching based on run-time environment, if we
        # were to pursue that in the future.
        cmd = scalems.executable(('/bin/echo',))
        # TODO: Check output
        session.run(cmd)


@pytest.mark.asyncio
async def test_exec_local(cleandir):
    # Test local execution with standard deferred launch.
    # TODO: The `with` block should be equivalent to a `-m scalems.local` invocation. Test.
    context = scalems.local.AsyncWorkflowManager()
    asyncio.get_event_loop().set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    # Note that a coroutine object created from an `async def` function is only awaitable once.
    with scalems.context.scope(context):
        # TODO: Input type checking.
        try:
            cmd = executable(('/bin/cat', '-'), stdin=('hi there\n',), stdout='stdout.txt')
        except Exception as e:
            raise
        assert isinstance(cmd, scalems.context.ItemView)
        # TODO: Future interface allows client to force resolution of dependencies.
        # cmd.result()
        # TODO:
        # scalems.run(cmd)
        # TODO: Remove Session.run() from public interface (use scalems.run())
        # await context.run()
        async with context.dispatch():
            ...
        result = cmd.result()  # type: scalems.subprocess.SubprocessResult
        assert result.stdout.name == 'stdout.txt'
        with open(result.stdout) as fh:
            assert fh.read().startswith('hi there')

# Currently in test_rp_exec.py
# def test_exec_rp():
#     # Test RPDispatcher context
#     # Note that a coroutine object created from an `async def` function is only awaitable once.
#     cmd = executable(('/bin/echo',))
#     context = sms_context.RPDispatcher()
#     with context as session:
#         session.run(cmd)