"""
Specialize implementations of ScaleMS operations.

"""
import scalems.subprocess


async def executable(task_input: 'radical.pilot.ComputeUnitDescription' = None):
    context = scalems_context.get_context()
    assert isinstance(context, scalems_context.RPDispatcher)
    assert context.umgr is not None
    task_description = {'executable': task_input['argv'][0],
                        'cpu_processes': 1}
    task = context.umgr.submit_units(context.rp.ComputeUnitDescription(task_description))
    # task.wait() just hangs. Using umgr.wait_units() instead...
    # task.wait()
    context.umgr.wait_units()
    assert task.exit_code == 0

def make_cu(context, task_input):
    """InputResource factory for implementations based on standard RP ComputeUnit.

    .. todo:: How can we defer run time argument realization? Is this what the RP "kernel" idea is for?
    """
    from scalems.context.radical import RPContextManager
    if not isinstance(context, RPContextManager):
        raise ValueError('This resource factory is only valid for RADICAL Pilot workflow contexts.')
    task_description = {'executable': task_input['argv'][0],
                        'cpu_processes': 1}
    return context.rp.ComputeUnitDescription(task_description)

