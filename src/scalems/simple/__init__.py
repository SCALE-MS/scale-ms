import radical.pilot as rp
import radical.utils as ru
import radical.saga as rs
import gmxapi
from functools import partial

def get_pilot_desc(resource: str = 'local.localhost'):
    description = rp.PilotDescription({'resource': resource,
                            'runtime'      : 30,  # pilot runtime minutes
               'exit_on_error': True,
               'project'      : None,
               'queue'        : None,
               'cores'        : 4,
               'gpus'         : 0,
               'exit_on_error': False})
    return description

class SimpleManager:
    """ create session and managers """
    def __init__(self):
        self._session = rp.Session()
        self._pilot_mgr    = rp.PilotManager(self._session)
        self._task_mgr    = rp.TaskManager(self._session)
        self._report = ru.Reporter(name='radical.pilot')
        self._report.title(f'Getting Started (RP version {rp.version})')


    def prepare_raptor(self, pilot_description, venv_path):
        self._report.header('submitting pilot')
        pilot = self._pilot_mgr.submit_pilots(pilot_description)
        env_name = 'local'
        env_spec = {"type": "venv", "path": venv_path, "setup": []}
        pilot.prepare_env(env_name=env_name, env_spec=env_spec)

        # add the pilot to the task manager and wait for the pilot to become active
        self._task_mgr.add_pilots(pilot)
        pilot.wait(rp.PMGR_ACTIVE)
        self._report.header('pilot is up and running')

        master_descr = {'mode': rp.RAPTOR_MASTER, 'named_env': env_name}
        worker_descr = {'mode': rp.RAPTOR_WORKER, 'named_env': env_name}

        self._raptor = pilot.submit_raptors( [rp.TaskDescription(master_descr)])[0]
        self._workers = self._raptor.submit_workers([rp.TaskDescription(worker_descr),rp.TaskDescription(worker_descr)])
        self._report.header('raptor is up and running')


    def submit_raptor(self, tasks_list):
        self._report.header('Submitting raptor tasks')
        tasks = self._raptor.submit_tasks(tasks_list)
        self._task_mgr.wait_tasks(get_task_info(tasks, 'uid'))
        return tasks

    def submit_task(self, tasks_list):
        self._report.header('Submitting tasks')
        tasks = self._task_mgr.submit_tasks(tasks_list)
        self._task_mgr.wait_tasks(get_task_info(tasks, 'uid'))
        return tasks

    def make_raptor_task(self, func):
        #import ipdb;ipdb.set_trace()
        @rp.pythontask
        def local(func):
            #type(func)==gmxapi.operation.OperationHandle
            return func
        return rp.TaskDescription({'mode': rp.TASK_FUNCTION,
                                   'function': local(func),})

    def make_exe_task(self, executable, args_list):
        return rp.TaskDescription({'mode': rp.TASK_EXECUTABLE,
                                   'executable': executable,
                                   'arguments': args_list,
                                   })

    def close(self):
        self._session.close()


def get_task_info(tasks, info):
    return [task.as_dict()[info] for task in tasks]

def get_task_path(task):
    return rs.Url(task.sandbox).path

