import radical.pilot as rp
import radical.utils as ru
import radical.saga as rs
import asyncio
import importlib
from dataclasses import dataclass

def get_pilot_desc(resource: str = 'local.localhost'):
    description = rp.PilotDescription({'resource': resource,
                            'runtime'      : 30,  # pilot runtime minutes
               'exit_on_error': False,
               'project'      : None,
               'queue'        : None,
               'cores'        : 4,
               'gpus'         : 0,})
    return description

class SimpleManager:
    """ create session and managers """
    def __init__(self, start_session: bool = True):
        self._report = ru.Reporter(name='radical.pilot')
        if start_session:
            self._radical_setup()

    def _radical_setup(self):
        session_cfg = rp.utils.get_resource_config(resource='local.localhost')
        session_cfg.agent_scheduler = "CONTINUOUS_ORDERED"
        self._session = rp.Session(cfg=session_cfg)
        self._pilot_mgr    = rp.PilotManager(self._session)
        self._task_mgr    = rp.TaskManager(self._session)
        self._report.title(f'Getting Started (RP version {rp.version})')

    def wait_tasks(self, tasks):
        self._task_mgr.wait_tasks(get_task_info(tasks, 'uid'))

    async def producer(self, task_list):
        print('Producer: Running')
        # generate work
        for task_description in task_list:
            # add to the queue
            #import ipdb;ipdb.set_trace()
            await self.queue.put(self._raptor.submit_tasks([task_description])[0])
        # wait for all items to be processed
        await self.queue.join()
        # send sentinel value
        await self.queue.put(None)
        print('Producer: Done')

    async def consumer(self):
        print('Consumer: Running')
        outputs = list()
        while True:
            # get a unit of work
            #import ipdb;ipdb.set_trace()
            item = await self.queue.get()
            # check for stop
            if item is None:
                break
            # block
            await asyncio.sleep(0.1)
            # report
            print(f'>got {item}')
            outputs.append(item)
            # mark it as processed
            self.queue.task_done()
        print('Consumer: Done')
        return outputs

    async def run_queue(self, task_list):
        self.queue = asyncio.Queue(maxsize=10)
        _, out = await asyncio.gather(self.producer(task_list), self.consumer())
        del self.queue
        return out

    def prepare_raptor(self, pilot_description, venv_path):
        self._report.header('submitting pilot')
        pilot = self._pilot_mgr.submit_pilots(pilot_description)
        self._env_name = 'local'
        env_spec = {"type": "venv", "path": venv_path, "setup": []}

        # add the pilot to the task manager and wait for the pilot to become active
        self._task_mgr.add_pilots(pilot)
        pilot.wait(rp.PMGR_ACTIVE)
        pilot.prepare_env(env_name=self._env_name, env_spec=env_spec)
        self._report.header('pilot is up and running')

        master_descr = {'mode': rp.RAPTOR_MASTER, 'named_env': self._env_name}
        worker_descr = {'mode': rp.RAPTOR_WORKER, 'named_env': self._env_name}

        self._raptor = pilot.submit_raptors( [rp.TaskDescription(master_descr)])[0]
        self._workers = self._raptor.submit_workers([rp.TaskDescription(worker_descr)])
        self._pilot = pilot
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
        @rp.pythontask
        def local(func):
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

@rp.pythontask
def run_gmxapi_radical(args, input_files, output_files):
    import gmxapi as gmx
    cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), args, input_files, output_files)
    return cmd.output.file.result()

@dataclass
class WorkItem:
    args: list
    """Positional arguments for *func*."""

    kwargs: dict
    """Key word arguments for *func*."""

    round_name: str

    step_number: int

    tasks_in_step: int

    func: str = run_gmxapi_radical.__name__
    """A callable to be retrieved as an attribute in *module*."""

    module: str = run_gmxapi_radical.__module__
    """The qualified name of a module importable by the Worker."""

def raptor_task_from_work_item(work_item: WorkItem, env_name: str = None):
    module = importlib.import_module(work_item.module)
    func = getattr(module, work_item.func)
    args = list(work_item.args)
    kwargs = work_item.kwargs.copy()

    tasks = list()
    for step in range(work_item.tasks_in_step):
        tags={'order': {'ns': work_item.round_name,
                        'order': work_item.step_number,
                        'size': work_item.tasks_in_step}}

        td = rp.TaskDescription()
        #td.tags = tags
        td.uid = f"{work_item.round_name}_step-{work_item.step_number}_task-{step}"

        outputs_to_stage = list()
        for fn in work_item.args[2].values():
            outputs_to_stage.append({'source': f"task:///gmxapi.commandline.cli0_i0/{fn}",
                              'target': f"client:///scalems_0_0/{td.uid}.{fn}",
                              'action': rp.TRANSFER})
        td.output_staging = outputs_to_stage

        td.ranks = 2
        td.cores_per_rank = 2

        td.mode = rp.TASK_FUNCTION
        td.function = func(*args, **kwargs)
        if env_name:
            td.named_env = env_name

        tasks.append(td)
    return tasks

def get_task_info(tasks, info):
    return [task.as_dict()[info] for task in tasks]

def get_task_path(task):
    return rs.Url(task.sandbox).path

