"""
Specialize implementations of ScaleMS operations.

"""
import weakref

import scalems.subprocess
from . import RPFuture, RPResult


def executable(context, task: scalems.subprocess.Subprocess):
    if not isinstance(context, scalems.radical.RPWorkflowContext):
        raise ValueError('This resource factory is only valid for RADICAL Pilot workflow contexts.')

    task_input = task.input_collection()
    args = list([arg for arg in task_input.argv])
    # TODO: stream based input with PIPE.
    kwargs = {
        'stdin': None,
        'stdout': None,
        'stderr': None,
        'env': None
    }

    task_description = {'executable': args[0],
                        'cpu_processes': 1}
    task = context.umgr.submit_units(context.rp.ComputeUnitDescription(task_description))
    task_ref = weakref.ref(task)
    # TODO: The Context should be in charge of creating the Future.
    future = RPFuture(task_ref)

    def cb(obj, state):
        # Where is the state enumeration?
        # TODO: assert state in [...]
        if task_ref().exit_code is not None:
            future.set_result(RPResult())

    task.register_callback(cb)

    async def coroutine():
        # task.wait() just hangs. Using umgr.wait_units() instead...
        # task.wait()
        context.umgr.wait_units()
        return future
    return coroutine()
