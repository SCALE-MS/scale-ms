"""Run a single command-line-based task wrapping /bin/echo

Example:

    $ python -m scalems.radical --venv=$HOME/testenv --resource=local.github \
      --access=ssh --pilot-option cores=1 \
      examples/basic/echo.py hello world
    $ cat 0*/stdout
    hi there
    $

Note that the examples above should be run in clean working directories or else
cached results may be returned without new execution.

TODO: Clarify user-specified output behavior.
Note that the examples above are not literally correct. As of January 2021, output
files land in an arbitrarily named subdirectory.
"""

import sys

import scalems


@scalems.app
def main():
    cmd = scalems.executable(argv=["/bin/echo"] + sys.argv[1:], stdout="stdout")
    # TODO: Allow Future slicing.
