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

import scalems

@scalems.app
def main():
    cmd = scalems.executable(argv=['/bin/echo'] + sys.argv[1:], stdout='stdout')
    # TODO: Allow Future slicing.
