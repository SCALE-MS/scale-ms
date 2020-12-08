"""
Specialize implementations of ScaleMS operations.

"""
import weakref

import scalems.subprocess
from scalems.core.exceptions import DispatchError, InternalError

from . import RPFuture, RPResult


def executable(context, task: scalems.subprocess.Subprocess):
    """Implement scalems.executable for the RPWorkflowContext.

    Provide the awaitable result for the Subprocess Future behavior.

    TODO: Tie return value to SubprocessResult.
    TODO: Manage the state of the Subprocess instance.
    TODO: Finish implementing Future.
    TODO: Move Future base to asyncio.Future.
    """
    if not isinstance(context, scalems.radical.RPWorkflowContext):
        raise DispatchError('This resource factory is only valid for RADICAL Pilot workflow contexts.')

    task_input = task.input_collection()
    args = list([arg for arg in task_input.argv])
    # TODO: stream based input with PIPE.
    kwargs = {
        'stdin': None,
        'stdout': None,
        'stderr': None,
        'env': None
    }

    # Construct the RP executable task description.
    # Ref: https://radicalpilot.readthedocs.io/en/stable/apidoc.html#radical.pilot.ComputeUnit
    task_description = {'executable': args[0],
                        'cpu_processes': 1}
    task = context.umgr.submit_units(context.rp.ComputeUnitDescription(task_description))
    task_ref = weakref.ref(task)
    # TODO: The Context should be in charge of creating the Future.
    try:
        future = RPFuture(task_ref)
    except TypeError as e:
        raise InternalError('Failed to get a reference to a new RADICAL Pilot task.') from e
    except Exception as e:
        raise InternalError('Unknown error occurred in RADICAL Pilot connector.') from e

    def cb(obj, state):
        # Where is the state enumeration?
        # TODO: assert state in [...]
        if task_ref().exit_code is not None:
            future.set_result(RPResult())

    task.register_callback(cb)

    async def coroutine():
        # task.wait() just hangs. Using umgr.wait_units() instead...
        # task.wait()
        # TODO: Why does task.wait() not work?
        # TODO: Can we at least wait on a specific task ID?
        context.umgr.wait_units()
        return future
    return coroutine()
