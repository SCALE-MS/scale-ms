"""Run a single command-line-based task wrapping /bin/echo

Example:

    $ python echo_detail.py

    $ ls -d */outfile.txt
    0000000000000000000000000000000000000000000000000000000000000000/outfile.txt
    $ rm -rf 000*

Example:

    $ python echo_detail.py hi there
    hi there
    $

Note that the examples above should be run in clean working directories or else
cached results may be returned without new execution.
"""

import asyncio
import logging
import sys

import typing
from pathlib import Path

import scalems
from scalems.local import workflow_manager
from scalems import executable


asyncio.get_event_loop().set_debug(True)
logging.getLogger("asyncio").setLevel(logging.DEBUG)


async def main(execution_context, words: typing.Iterable[str] = ()):
    try:
        # Using `cat` instead of `echo` to demonstrate availability of stdin.
        cmd = executable(('/bin/cat', '-'), stdin=(' '.join(words) + '\n\n',), stdout='outfile.txt')
    except Exception as e:
        raise
    assert isinstance(cmd, scalems.context.ItemView)
    # TODO: Future interface allows client to force resolution of dependencies.
    # cmd.result()
    # TODO: #82
    # scalems.run(cmd)
    # TODO: Remove Session.run() from public interface (use scalems.run())
    # await context.run()
    async with execution_context.dispatch():
        ...
    # WARNING: If task appears to have already run, it is not re-executed,
    # but we have not yet implemented task state restoration from existing output,
    # so running multiple times without cleaning up output directories will cause
    # scalems.context.InvalidStateError: Called result() on a Task that is not done.
    result = cmd.result()  # type: scalems.subprocess.SubprocessResult
    assert result.stdout.name == 'outfile.txt'
    path = Path(result.stdout)
    assert path.exists()
    return path


if __name__ == '__main__':
    manager = workflow_manager(asyncio.get_event_loop())
    with scalems.context.scope(manager) as context:
        outfile = asyncio.run(main(context, sys.argv[1:]))
    with open(outfile, 'r') as fh:
        for line in fh:
            print(line.rstrip())
