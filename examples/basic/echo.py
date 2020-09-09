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

import sys

import scalems.context

print(scalems.context.get_context())
print(sys.argv)

cmd = scalems.executable('/bin/echo', sys.argv[1:])

# TODO: TaskView
# cmd.result()

# TODO: TaskView
# scalems.run(cmd)