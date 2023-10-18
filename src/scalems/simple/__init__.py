import radical.pilot as rp
import radical.utils as ru
import radical.saga as rs
import asyncio
import gmxapi
from functools import partial
from random import random

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
        self._task_queue = list()


    def _radical_setup(self):
        self._session = rp.Session()
        self._pilot_mgr    = rp.PilotManager(self._session)
        self._task_mgr    = rp.TaskManager(self._session)
        self._report.title(f'Getting Started (RP version {rp.version})')

    def add_raptor_task(self, raptor_task):
        self._task_queue.append(raptor_task)

    async def process_work(self):
        tasks = self._raptor.submit_tasks(self._task_queue)
        await self._task_mgr.wait_tasks(get_task_info(tasks, 'uid'))
        return tasks

    async def producer(self, item_list):
        print('Producer: Running')
        # generate work
        tasks = list()
        for item in item_list:
            # add to the queue
            #print(f"before: {item}")
            @rp.pythontask
            def local(func):
                return func
            @rp.pythontask
            def l2(func, kwargs):
                return func(**kwargs)
            import radical.pilot.pytask as pytask
            module = importlib.import_module(item["module"])
            func = getattr(module, item["func"])
            #localfunc=local(func)
            args = list(item["args"])
            """
            td1 = rp.TaskDescription({
                'mode': rp.TASK_FUNCTION,
                'function': run_in_worker,
                'args': [],
                'kwargs': {"work_item": item},
            })
            # run_in_worker not found
            # t1=self._raptor.submit_tasks([td1])[0]
            td2 = rp.TaskDescription({
                'mode': rp.TASK_FUNCTION,
                'function': local(run_in_worker),
                'args': [],
                'kwargs': {"work_item": item},
                'named_env': self._env_name,
            })
            t2=self._raptor.submit_tasks([td2])[0]
            td3 = rp.TaskDescription({
                'mode': rp.TASK_FUNCTION,
                'function': pytask.PythonTask(run_in_worker),
                'args': [],
                'kwargs': {"work_item": item},
            })
            t3=self._raptor.submit_tasks([td3])[0]
            td4 = rp.TaskDescription({
                'mode': rp.TASK_FUNCTION,
                'function': localfunc,
                'args': args,
                'kwargs': None,
                'named_env': self._env_name,
            })
            t4=self._raptor.submit_tasks([td4])[0]
            mypartial=partial(func, *item['args'])
            td5 = rp.TaskDescription({
                'mode': rp.TASK_FUNCTION,
                'function': mypartial,
                'args': [],
                'kwargs': None,
                'named_env': self._env_name,
            })
            t5=self._raptor.submit_tasks([td5])[0]
            td6 = rp.TaskDescription({
                'mode': rp.TASK_FUNCTION,
                'function': func(*item['args']),
                'args': [],
                'kwargs': None,
                'named_env': self._env_name,
            })
            t6=self._raptor.submit_tasks([td6])[0]
            """
            td7 = rp.TaskDescription()
            td7.mode=rp.TASK_FUNCTION
            td7.function=func(*item['args'])
            td7.named_env=self._env_name
            #t7=self._raptor.submit_tasks([td7])[0]
            #import ipdb;ipdb.set_trace()
            task = await self.queue.put(self._raptor.submit_tasks([td7])[0])
            tasks.append(task)
            #await self.lifo_queue.put(run_in_worker(work_item=item))
            #print(f"after: {item}")
        # wait for all items to be processed
        await self.queue.join()
        # send sentinel value
        await self.queue.put(None)
        print('Producer: Done')
        return tasks

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

    async def run_queue(self, item_list):
        self.queue = asyncio.Queue(maxsize=10)
        return await asyncio.gather(self.producer(item_list), self.consumer())

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

import importlib
from scalems.radical.raptor import ScalemsRaptorWorkItem
def run_in_worker(work_item: ScalemsRaptorWorkItem, comm=None):
        module = importlib.import_module(work_item["module"])
        func = getattr(module, work_item["func"])
        args = list(work_item["args"])
        kwargs = work_item["kwargs"].copy()
        comm_arg_name = work_item.get("comm_arg_name", None)
        if comm_arg_name is not None:
            if comm_arg_name:
                kwargs[comm_arg_name] = comm
            else:
                args.append(comm)
        print(
            "Calling {func} with args {args} and kwargs {kwargs}",
            {"func": func.__qualname__, "args": repr(args), "kwargs": repr(kwargs)},
        )
        return func(*args, **kwargs)

def get_task_info(tasks, info):
    return [task.as_dict()[info] for task in tasks]

def get_task_path(task):
    return rs.Url(task.sandbox).path

@rp.pythontask
def run_gmxapi_radical(args, input_files, output_files):
    import gmxapi as gmx
    cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), args, input_files, output_files)
    return cmd.output.file.result()

def run_gmxapi(args, input_files, output_files):
    import gmxapi as gmx
    cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), args, input_files, output_files)
    return cmd.output.file.result()

