===================
SCALE-MS invocation
===================

Users define and execute SCALE-MS workflows by using Python to define work and
submit it for execution through a SCALE-MS workflow manager.
The SCALE-MS machinery is accessible through the :py:mod:`scalems` Python module.

For the greatest flexibility in execution, scripts should be written without
explicit reference to the execution environment. Instead, a SCALE-MS workflow
manager :ref:`backend` can be specified on the command line to bootstrap an entry point.

:command:`python3 -m scalems.local myscript.py`
would use the workflow manager provided by the :py:mod:`scalems.local`
module to process :file:`myscript.py`. After the module performs some initialization,
the script is essentially just imported. After that, though, specifically annotated
callables (functions or function objects) are identified and submitted for execution.
See :py:func:`scalems.app`.

Command line execution
======================

Use the ``--help`` command line option for an execution module for details about
available and required command line arguments::

    $ python -m scalems.local --help
    usage: python -m scalems.local <scalems.local args> script-to-run.py.py <script args>
    ...

The base command line parser is provided by :py:func:`scalems.utility.parser`,
extended (optionally) by the :ref:`backend`, and further extended by
:py:func:`scalems.invocation.run`. Get usage for a particular backend with
reference to the particular module.

    python3 -m scalems.local --help

Unrecognized command line arguments will be passed along to the called script.

Documentation for built-in execution modules is shown below.
Documentation also may be accessed from the command line with
`pydoc <https://docs.python.org/3/library/pydoc.html>`__
or from within the interpreter with :py:func:`help`.
(E.g. ``pydoc scalems.radical``)

.. autoprogram:: scalems.local:parser

.. automodule:: scalems.local

.. autoprogram:: scalems.radical:parser

.. automodule:: scalems.radical

Pure Python execution
=====================

For some use cases (such as Jupyter notebooks), it may be preferable to configure the execution target and launch
a workflow entirely from within Python.

Such use cases are not yet well-supported in `scalems`.

Refer to the `test suite <https://github.com/SCALE-MS/scale-ms/tree/master/tests>`__ for examples,
or follow https://github.com/SCALE-MS/scale-ms/issues/82
