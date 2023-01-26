"""Run a chain of tasks that propagate some text.

Example::

    $ python echo-pipeline.py <scalems.radical options> -o stdout.txt some text
    $ cat stdout.txt
    some text
    $

To do:
    The example we think we want looks more like::

        $ python -m scalems.radical <rp options> echo-pipeline.py -o stdout.txt text
        $ cat stdout.txt
        text
        $

Reference RADICAL Pilot chained tasks example:
https://github.com/radical-cybertools/radical.pilot/blob/devel/examples/docs/chained_tasks.py

For every task A_n a task B_n is started consecutively.

Try running this example with RADICAL_PILOT_VERBOSE=debug set if
you want to see what happens behind the scenes!
"""

import argparse
import asyncio
import os
import sys
import typing

import scalems.radical
import scalems.call


verbose = os.environ.get("RADICAL_PILOT_VERBOSE", "REPORT")
os.environ["RADICAL_PILOT_VERBOSE"] = verbose


# Inherit from the backend parser so that `parse_known_args` can handle positional arguments the way we want.
parser = argparse.ArgumentParser(parents=[scalems.radical.parser], add_help=False)
parser.add_argument("-o", type=str, help="Output file name.")


def sender(text: typing.Sequence[str]) -> "tuple[str, ...]":
    """The first task emits the text from the CLI."""
    print(f"sender got {text}")
    if isinstance(text, str):
        raise ValueError
    else:
        return tuple(text)


def receiver(text: tuple[str, ...]) -> "dict[str, str]":
    """The second task receives the text from the first task."""
    return {"received": " ".join(text)}


_ResultT = typing.TypeVar("_ResultT")


class TaskHandle(typing.Generic[_ResultT]):
    def __init__(self, result_future: typing.Awaitable[scalems.call.Result]):
        self._result_future = result_future

    @classmethod
    async def submit(
        cls,
        *,
        func: typing.Callable[..., _ResultT],
        label: str,
        manager: scalems.workflow.WorkflowManager,
        dispatcher: scalems.radical.runtime.RPDispatchingExecutor,
        args=(),
        kwargs=None,
    ) -> "TaskHandle[_ResultT]":
        if kwargs is None:
            kwargs = {}
        task_uid = label
        call_handle: scalems.call._Subprocess = await scalems.call.function_call_to_subprocess(
            func=func, args=args, kwargs=kwargs, label=task_uid, manager=manager
        )
        rp_task_result: scalems.radical.runtime.RPTaskResult = await scalems.radical.runtime.subprocess_to_rp_task(
            call_handle, dispatcher=dispatcher
        )
        result_future = scalems.radical.runtime.subprocess_result_from_rp_task(call_handle, rp_task_result)
        return cls(result_future)

    async def result(self) -> _ResultT:
        call_result: scalems.call.Result = await self._result_future
        return call_result.return_value


async def main(text, manager: scalems.workflow.WorkflowManager):
    session: scalems.radical.runtime.RPDispatchingExecutor
    async with manager.dispatch() as session:
        # submit a single pipeline task to pilot job
        # task_handle = await TaskHandle.submit(
        #     func=sender,
        #     args=(text,),
        #     label=f"sender-1",
        #     manager=manager,
        #     dispatcher=session,
        # )
        # print(task_handle)
        # result = await task_handle.result()
        # print(result)
        # task_handle = await TaskHandle.submit(
        #     func=receiver, kwargs={"text": result}, label=f"receiver-1", manager=manager, dispatcher=session
        # )
        # print(task_handle)
        # result = await task_handle.result()
        # print(result)

        # TODO: more than 1 chain doesn't work because shutil.make_archive is not thread-safe.
        # NUMBER_CHAINS = 2
        NUMBER_CHAINS = 1

        tasks_A = tuple(
            asyncio.create_task(
                TaskHandle.submit(func=sender, args=(text,), label=f"sender-{i}", manager=manager, dispatcher=session),
                name=f"submit-{i}",
            )
            for i in range(NUMBER_CHAINS)
        )
        # wait for submission
        handles_A = await asyncio.gather(*tasks_A)

        # Localize all the results.
        # For periodic batches, see asyncio.with()
        # For proxy access to next-available iterative chaining, use asyncio.as_completed()
        results_A = await asyncio.gather(*tuple(typing.cast(TaskHandle, handle).result() for handle in handles_A))
        # assert isinstance(handles_A[0], TaskHandle)
        # result = await handles_A[0].result()
        # print(result)

        # Wait for submission
        tasks_B = await asyncio.gather(
            *tuple(
                asyncio.create_task(
                    TaskHandle.submit(
                        func=receiver,
                        kwargs={"text": result},
                        label=f"receiver-{i}",
                        manager=manager,
                        dispatcher=session,
                    ),
                    name=f"receiver-{i}",
                )
                for i, result in enumerate(results_A)
            )
        )

        results_B = [
            asyncio.create_task(typing.cast(TaskHandle, task).result(), name=f"results-{i}")
            for i, task in enumerate(tasks_B)
        ]
        # Handle results as they come in.
        return [await coro for coro in asyncio.as_completed(results_B)]


if __name__ == "__main__":
    # Work around a quirk: we are using the parser that normally assumes the
    # backend from the command line. We can switch back to the `-m scalems.radical`
    # style invocation when we have some more updated UI tools
    # (e.g. when scalems.wait has been updated) and `main` doesn't have to be a coroutine.
    sys.argv.insert(0, __file__)

    config, argv = parser.parse_known_args()
    outfile = config.o
    if not outfile:
        outfile = "stdout.txt"

    manager = scalems.radical.workflow_manager(asyncio.get_event_loop())
    with scalems.workflow.scope(manager, close_on_exit=True):
        results = asyncio.run(main(argv, manager))

    with open(outfile, "w") as fh:
        fh.writelines(result["received"] + "\n" for result in results)
