"""Run a single command-line-based task wrapping /bin/echo

Example:

    $ python -m scalems.local echo.py
    $ cat stdout

    $

Example:

    $ python -m scalems.local echo.py hi there
    $ cat stdout
    hi there
    $

Example:

    $ python -m scalems.radical echo.py hi there
    $ cat stdout
    hi there
    $

Note that the examples above should be run in clean working directories or else
cached results may be returned without new execution.
"""

import radical.pilot

import logging
import sys

import scalems.context
import scalems.local

character_stream = logging.StreamHandler()
# Optional: Set log level.
logging.getLogger('scalems').setLevel(logging.DEBUG)
character_stream.setLevel(logging.DEBUG)
# Optional: create formatter and add to character stream handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
character_stream.setFormatter(formatter)
# add handler to logger
logging.getLogger('scalems').addHandler(character_stream)


def main():
    cmd = scalems.executable(argv=['/bin/echo'] + sys.argv[1:])
    # TODO: Allow Future slicing.
    return cmd
    # TODO: Automatically dispatch on call to `result()`
    # cmd_result = cmd.result()
    # with open(cmd_result.stdout, 'r') as fh:
    #     for line in fh:
    #         print(line.rstrip())
    # For `cmd.result()`, we have to decide how to block until a sufficient amount
    # of asyncio work has taken place. We might need to decorate this enclosing function
    # to allow it to yield to the asyncio event loop. Alternatively, we can embed
    # functionality in `scalems.wait` that will look for a running event loop and
    # take appropriate action, which may mean throwing an error. We would have to use
    # low level functionality like loop.run_until_complete() and loop.stop() within
    # some scalems environment tracking the global event loop.


# Only run the following when this script is executed directly. (I.e. not with `python -m scalems.local ...`)
if __name__ == '__main__':
    context = scalems.local.AsyncWorkflowManager()
    cmd = scalems.run(main, context=context)
    cmd_result = cmd.result()
    with open(cmd_result.stdout, 'r') as fh:
        for line in fh:
            print(line.rstrip())
