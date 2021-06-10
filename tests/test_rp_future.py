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

    td = rp.TaskDescription({'executable': '/bin/bash',
                             'arguments': ['-c', '/bin/sleep 5 && /bin/echo success'],
                             'cpu_processes': 1})

    # Test propagation of RP cancellation behavior
    task: rp.Task = tmgr.submit_tasks(td)

    future = asyncio.get_running_loop().create_future()
    wrapper: asyncio.Future = await scalems.radical.rp_task(task, future)

    task.cancel()
    try:
        # TODO: With Python 3.9, check cancellation message for how the cancellation propagated.
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(wrapper, timeout=120)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e

    assert future.cancelled()
    assert wrapper.cancelled()
    assert task.state == rp.states.CANCELED

    # Test propagation of asyncio watcher task cancellation.
    task: rp.Task = tmgr.submit_tasks(td)

    future = asyncio.get_running_loop().create_future()
    wrapper: asyncio.Task = await scalems.radical.rp_task(task, future)

    assert isinstance(wrapper, asyncio.Task)
    wrapper.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(wrapper, timeout=5)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e
    assert wrapper.cancelled()
    assert future.cancelled()

    # WARNING: This blocks. Don't do it in the event loop thread.
    task.wait()
    # Note that if the test is paused by a debugger, the rp task may
    # have a chance to complete before being canceled.
    # Ordinarily, that will not happen in this test.
    # assert task.state in (rp.states.CANCELED, rp.states.DONE)
    assert task.state in (rp.states.CANCELED,)

    # Test propagation of asyncio future cancellation.
    task: rp.Task = tmgr.submit_tasks(td)

    future = asyncio.get_running_loop().create_future()
    wrapper: asyncio.Task = await scalems.radical.rp_task(task, future)

    assert isinstance(wrapper, asyncio.Task)
    future.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(future, timeout=5)
        await asyncio.wait_for(wrapper, timeout=1)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e
    assert not wrapper.cancelled()
    assert future.cancelled()

    # WARNING: This blocks. Don't do it in the event loop thread.
    task.wait()
    # Note that if the test is paused by a debugger, the rp task may
    # have a chance to complete before being canceled.
    # Ordinarily, that will not happen in this test.
    # assert task.state in (rp.states.CANCELED, rp.states.DONE)
    assert task.state in (rp.states.CANCELED,)

    # Test run to completion
    task: rp.Task = tmgr.submit_tasks(td)

    future = asyncio.get_running_loop().create_future()
    wrapper: asyncio.Task = await scalems.radical.rp_task(task, future)

    timeout = 120
    try:
        result = await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.debug(f'Waited more than {timeout} for {future}: {e}')
        result = None
    assert task.exit_code == 0
    assert 'success' in task.stdout

    assert 'stdout' in result
    assert 'success' in result['stdout']
    assert wrapper.done()

    # Test failure handling
    # TODO: Use a separate test for results and error handling.


@pytest.mark.skip(reason='Unimplemented.')
@pytest.mark.asyncio
async def test_chained_commands():
    """Run a sequence of two tasks with a data flow dependency."""
    assert False
