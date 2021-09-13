"""Test dependency-management wrappers for RP.

Note: `export RADICAL_LOG_LVL=DEBUG` to enable RP debugging output.
"""
import asyncio
import logging

import pytest

import scalems
import scalems.context
import scalems.radical
import scalems.radical.runtime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# TODO: Catch sigint from RP and apply our own timeout.


@pytest.mark.asyncio
async def test_rp_future(rp_task_manager):
    """Check our Future implementation.

    Fulfill the asyncio.Future protocol for a rp.Task wrapper object. The wrapper
    should appropriately yield when the rp.Task is not finished.
    """
    import radical.pilot as rp

    tmgr = rp_task_manager

    td = rp.TaskDescription({
                                'executable': '/bin/bash',
                                'arguments': ['-c', '/bin/sleep 5 && /bin/echo success'],
                                'cpu_processes': 1
                            })

    # Test propagation of RP cancellation behavior
    task: rp.Task = tmgr.submit_tasks(td)

    rp_future: asyncio.Future = await scalems.radical.rp_task(task)

    task.cancel()
    try:
        # TODO: With Python 3.9, check cancellation message for how the cancellation
        #  propagated.
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(rp_future, timeout=120)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e

    assert rp_future.cancelled()
    assert task.state == rp.states.CANCELED

    # Test propagation of asyncio watcher task cancellation.
    task: rp.Task = tmgr.submit_tasks(td)

    rp_future: asyncio.Task = await scalems.radical.rp_task(task)

    assert isinstance(rp_future, asyncio.Task)
    rp_future.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(rp_future, timeout=5)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e
    assert rp_future.cancelled()

    # WARNING: This blocks. Don't do it in the event loop thread.
    task.wait()
    # Note that if the test is paused by a debugger, the rp task may
    # have a chance to complete before being canceled.
    # Ordinarily, that will not happen in this test.
    # assert task.state in (rp.states.CANCELED, rp.states.DONE)
    assert task.state in (rp.states.CANCELED,)

    # Test run to completion
    task: rp.Task = tmgr.submit_tasks(td)

    rp_future: asyncio.Task = await scalems.radical.rp_task(task)

    timeout = 120
    try:
        result: rp.Task = await asyncio.wait_for(rp_future, timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.debug(f'Waited more than {timeout} for {rp_future}: {e}')
        raise e
    assert task.exit_code == 0
    assert 'success' in task.stdout

    assert hasattr(result, 'stdout')
    assert 'success' in result.stdout
    assert rp_future.done()

    # Test failure handling
    # TODO: Use a separate test for results and error handling.


@pytest.mark.skip(reason='Unimplemented.')
@pytest.mark.asyncio
async def test_chained_commands():
    """Run a sequence of two tasks with a data flow dependency."""
    assert False
