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
import scalems.workflow


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
    def __init__(
        self,
        *,
        func: typing.Callable[..., _ResultT],
        label: str,
        manager: scalems.workflow.WorkflowManager,
        dispatcher: scalems.radical.runtime.RPDispatchingExecutor,
        args: tuple = (),
        kwargs: dict = None,
        requirements: dict = None,
    ):
        if kwargs is None:
            kwargs = {}
        task_uid = label
        if requirements is None:
            requirements = {}
        self._call_handle: asyncio.Task[scalems.call._Subprocess] = asyncio.create_task(
            scalems.call.function_call_to_subprocess(
                func=func, label=task_uid, args=args, kwargs=kwargs, manager=manager, requirements=requirements
            )
        )
        self._dispatcher = dispatcher

    async def result(self) -> _ResultT:
        # Wait for input preparation
        call_handle = await self._call_handle
        rp_task_result_future = asyncio.create_task(
            scalems.radical.runtime.subprocess_to_rp_task(call_handle, dispatcher=self._dispatcher)
        )
        # Wait for submission and completion
        rp_task_result = await rp_task_result_future
        result_future = asyncio.create_task(
            scalems.radical.runtime.wrapped_function_result_from_rp_task(call_handle, rp_task_result)
        )
        # Wait for results staging.
        result = await result_future
        return result.return_value


async def main(text, manager: scalems.workflow.WorkflowManager, size: int):
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

        # scalems.submit(...)
        # TODO: Group submissions for lower overhead.
        tasks_A = tuple(
            TaskHandle(
                func=sender,
                label=f"sender-{i}",
                manager=manager,
                dispatcher=session,
                args=(text,),
                requirements={"ranks": 2, "cores_per_rank": 2, "threading_type": "OpenMP"},
            )
            for i in range(size)
        )

        # Localize all the results.
        # For individual tasks, see asyncio.wait_for(), which has a timeout optional kwarg.
        # For periodic batches, see asyncio.wait()
        # For proxy access to next-available iterative chaining, use asyncio.as_completed()
        results_A = await asyncio.gather(*tuple(handle.result() for handle in tasks_A))

        tasks_B = tuple(
            TaskHandle(
                func=receiver, label=f"receiver-{i}", manager=manager, dispatcher=session, kwargs={"text": result}
            )
            for i, result in enumerate(results_A)
        )

        results_B = [asyncio.create_task(task.result(), name=f"results-{i}") for i, task in enumerate(tasks_B)]
        # Handle results as they come in.
        return [await coro for coro in asyncio.as_completed(results_B)]


if __name__ == "__main__":
    import logging

    # Inherit from the backend parser so that `parse_known_args` can handle positional arguments the way we want.
    parser = argparse.ArgumentParser(parents=[scalems.radical.parser], add_help=False)
    parser.add_argument("-o", type=str, help="Output file name.")
    parser.add_argument("--size", type=int, default=10, help="Ensemble size: number of parallel pipelines.")

    # Work around some quirks: we are using the parser that normally assumes the
    # backend from the command line. We can switch back to the `-m scalems.radical`
    # style invocation when we have some more updated UI tools
    # (e.g. when scalems.wait has been updated) and `main` doesn't have to be a coroutine.
    sys.argv.insert(0, __file__)

    config, argv = parser.parse_known_args()

    level = None
    debug = False
    if config.log_level is not None:
        level = logging.getLevelName(config.log_level)
        debug = level <= logging.DEBUG
    if level is not None:
        character_stream = logging.StreamHandler()
        logging.getLogger("scalems").setLevel(level)
        logging.getLogger("asyncio").setLevel(level)
        character_stream.setLevel(level)
        formatter = logging.Formatter("%(asctime)s-%(name)s:%(lineno)d-%(levelname)s - %(message)s")
        character_stream.setFormatter(formatter)
        logging.getLogger("scalems").addHandler(character_stream)
        logging.getLogger("asyncio").addHandler(character_stream)

    verbose = os.environ.get("RADICAL_PILOT_VERBOSE", "REPORT")
    os.environ["RADICAL_PILOT_VERBOSE"] = verbose

    outfile = config.o
    if not outfile:
        outfile = "stdout.txt"

    manager = scalems.radical.workflow_manager(asyncio.get_event_loop())
    with scalems.workflow.scope(manager, close_on_exit=True):
        results = asyncio.run(main(argv, manager, size=config.size), debug=debug)

    with open(outfile, "w") as fh:
        fh.writelines(result["received"] + "\n" for result in results)
