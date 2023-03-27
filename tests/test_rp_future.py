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

    timeout = 120

    tmgr = rp_task_manager

    task_description_dict = dict(
        executable="/bin/bash",
        arguments=["-c", "/bin/sleep 5 && /bin/echo success"],
        cpu_processes=1,
    )
    task_description = rp.TaskDescription(from_dict=task_description_dict)
    task_description.uid = "test-rp-future-1"

    # Test propagation of RP cancellation behavior
    task: rp.Task = tmgr.submit_tasks(task_description)

    rp_future: asyncio.Future = await scalems.radical.runtime.rp_task(task)

    task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(rp_future, timeout=timeout)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e

    assert rp_future.cancelled()
    assert task.state == rp.states.CANCELED

    # Test propagation of asyncio watcher task cancellation.
    task_description.uid = "test-rp-future-2"
    task: rp.Task = tmgr.submit_tasks(task_description)

    rp_future: asyncio.Task = await scalems.radical.runtime.rp_task(task)

    assert isinstance(rp_future, asyncio.Task)
    rp_future.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(rp_future, timeout=5)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e
    assert rp_future.cancelled()

    # WARNING: rp.Task.wait blocks, and may never complete. Don't do it in the event loop thread.
    watcher = asyncio.create_task(asyncio.to_thread(task.wait, timeout=timeout), name=f"watch_{task.uid}")
    try:
        state = await asyncio.wait_for(watcher, timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.exception(f"Waited more than {timeout} for {watcher}")
        watcher.cancel("Canceled after waiting too long.")
        raise e
    else:
        assert state in rp.states.FINAL
    # Note that if the test is paused by a debugger, the rp task may
    # have a chance to complete before being canceled.
    # Ordinarily, that will not happen in this test.
    # assert task.state in (rp.states.CANCELED, rp.states.DONE)
    assert task.state in (rp.states.CANCELED,)

    # Test run to completion
    task_description.uid = "test-rp-future-3"
    watcher = asyncio.create_task(asyncio.to_thread(tmgr.submit_tasks, task_description), name="rp_submit")
    try:
        task: rp.Task = await asyncio.wait_for(watcher, timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.exception(f"Waited more than {timeout} to submit {task_description}.")
        watcher.cancel()
        raise e

    rp_future: asyncio.Task[rp.Task] = await scalems.radical.runtime.rp_task(task)
    try:
        result: rp.Task = await asyncio.wait_for(rp_future, timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.debug(f"Waited more than {timeout} for {rp_future}: {e}")
        rp_future.cancel("Canceled after waiting too long.")
        raise e
    assert task.exit_code == 0
    assert "success" in task.stdout

    assert hasattr(result, "stdout")
    assert "success" in result.stdout
    assert rp_future.done()

    # Test failure handling
    # TODO: Use a separate test for results and error handling.


@pytest.mark.skip(reason="Unimplemented.")
@pytest.mark.asyncio
async def test_chained_commands():
    """Run a sequence of two tasks with a data flow dependency."""
    assert False
