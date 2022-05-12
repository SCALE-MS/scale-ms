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

.. _backend:

Execution Modules
=================

`scalems` provides the following built-in execution modules.

scalems.local Python module
---------------------------

.. automodule:: scalems.local

.. _scalems.local command line:

.. autoprogram:: scalems.local:parser

scalems.radical Python module
-----------------------------

.. automodule:: scalems.radical

.. _scalems.radical command line:

.. autoprogram:: scalems.radical:parser

.. _venvs:

More notes on Python virtual environments
-----------------------------------------

Pilot environment
~~~~~~~~~~~~~~~~~

The RADICAL Pilot remote software components launch from a Python virtual
environment determined by parameters in the :ref:`RP resource` definition.

By default, RADICAL Pilot resources are configured to bootstrap the target
environment by creating a fresh virtual environment.
(``virtenv_mode=create`` and ``rp_version=local`` in most
`resource <https://radicalpilot.readthedocs.io/en/stable/machconf.html>`__
definitions.)
``virtenv_mode=update`` is a better choice than ``create``, so that later
sessions can re-use a previously bootstrapped pilot venv.

.. _static pilot venv:

Static Pilot venv
"""""""""""""""""

To minimize the amount of bootstrapping RP performs for each
:py:class:`~radical.pilot.Session`,
you can set up a completely static set of virtual environments with customized
resource definitions in :file:`$HOME/.radical/pilot/configs/`.
Configure the :ref:`RP resource` to *use* an existing *virtenv* and the RP
installation it contains.
Set ``virtenv_mode=use``, ``virtenv=/path/to/venv``, ``rp_version=installed``
in the RP resource definition.

.. note::
    This optimization is relevant even for the ``local.localhost`` resource
    and ``local`` access scheme!

The user (or client) is then responsible for maintaining venv(s) with the
correct RCT stack (matching the API used by the client-side RCT stack).
Optionally, the same static venv *may* be used for task execution (see below),
in which case the user must also maintain a compatible `scalems` installation,
along with any other software dependencies of the workflow.

Task environment
~~~~~~~~~~~~~~~~

shell command injection
"""""""""""""""""""""""

RP TaskDescriptions allow environment preparation with lines of shell commands
using :py:attr:`~radical.pilot.TaskDescription.pre_exec`,
:py:attr:`~radical.pilot.TaskDescription.pre_launch`,
and :py:attr:`~radical.pilot.TaskDescription.pre_rank`.
(Note that, in addition to the attribute descriptions,
RP docs include further discussion at the bottom of the
:py:class:`~radical.pilot.TaskDescription` class documentation section.)

.. seealso:: https://github.com/SCALE-MS/scale-ms/issues/203
    for discussion on whether/how to expose this through `scalems.radical`.

static Task venv
""""""""""""""""

Use :option:`--venv <scalems.radical --venv>` to specify the virtual environment
in which tasks should execute at the target `resource`.
The user is responsible for ensuring a compatible `scalems` installation in the
target venv, as well as for satisfying any other workflow software dependencies.

.. warning:: When maintaining a venv for task execution, keep the RCT stack synchronized.

    The `scalems` package depends on the RADICAL packages, but it is important that
    the RCT stack in the :py:class:`~radical.pilot.raptor.Worker` environment is
    compatible with that in the Pilot agent environment and the client environment.
    If the Pilot resource is set to ``update`` (see `above <static pilot venv>`),
    the agent environment will be updated to the client-side versions automatically.
    When the task uses a separate environment,
    the user must separately update the environment named by
    :option:`--venv <scalems.radical --venv>`.

    Ultimately, `scalems.radical` will provide more automatic assistance for this.
    (See `https://github.com/SCALE-MS/scale-ms/issues/141`__). In the mean time,
    users should be aware that they need to update remote RADICAL installations
    whenever they update their client-side installation.

To reproduce the environment seen by your Tasks when interactively using the
static venv, be sure to *activate* the venv.

.. Note: the agent venv is not fully hidden in RP 1.14:
   https://github.com/radical-cybertools/radical.pilot/issues/2609

If you are using a static venv for the Pilot resource,
you may specify the :ref:`Pilot venv <venvs>` path to
:option:`--venv <scalems.radical --venv>`.
You still must make sure that the venv provides `scalems` and the other
workflow software dependencies.
If you are using a dynamically maintained Pilot venv (``create`` or ``update``),
then you should use a separate venv for your tasks.

.. note::
    The :option:`scalems.radical --venv` option is intended to be optional.
    See https://github.com/SCALE-MS/scale-ms/issues/90 and
    https://github.com/SCALE-MS/scale-ms/issues/141

named_env
"""""""""

`scalems.radical` is migrating towards more dynamic and automated Python
environment preparation for workflow tasks.

RADICAL Pilot now allows a :py:class:`~radical.pilot.Task` some explicitly
Python-aware environment preparation,
(though users are still free to activate Task venvs using
:py:data:`~radical.pilot.TaskDescription.pre_exec`).

TaskDescription may use :py:attr:`~radical.pilot.TaskDescription.named_env`
to identify a virtual environment to be activated for the Task.
The virtual environment may be an existing virtual environment,
or a new environment.

In either case, to use *named_env*, :py:func:`~radical.pilot.Pilot.prepare_env`
*must* be called to register the named environment.

.. warning:: *prepare_env()* may not be lead to hard-to-diagnose states
    with invalid virtual environments.
    https://github.com/radical-cybertools/radical.pilot/issues/2589 describes
    incompletely provisioned new virtual environments. But similar symptoms
    can occur when trying to reference existing virtual environments.

See https://github.com/SCALE-MS/scale-ms/issues/90 for discussion on how
environments are named and provisioned, and how they are made available to tasks.

In addition to supporting :py:attr:`~radical.pilot.TaskDescription.named_env`
and the other task environment hooks,
:py:class:`~radical.pilot.raptor.Master` and
:py:class:`~radical.pilot.raptor.Worker`
tasks have some of the RP stack injected into their environment.
Raptor tasks are executed in new processes that are launched by the Worker
interpreter process through various mechanisms, depending on task requirements.
Various possible launch methods include forking from the
Worker interpreter process.
In other words, assumptions about the task Python environment are complicated,
and it is best if we try to base the task environment on the Worker environment.

.. seealso:: Provisioning Workers for (groups of) Tasks.
    `Issue #93 <https://github.com/SCALE-MS/scale-ms/issues/93>`__.

:py:mod:`scalems.radical` dispatches (most) tasks through
raptor "call" mode, so it constructs and uses a venv for the Worker
(*work in progress:*
`Issue #90 <https://github.com/SCALE-MS/scale-ms/issues/90>`__),
and **must not** specify ``named_env`` for work load tasks.

`scalems` will be unable to infer all software dependencies, such as special
package builds, or software managed outside of a supported Python package
management system
(e.g. CMake-driven LAMMPS installation, Plumed-enabled GROMACS).
It is not yet clear in what way and to what extent `scalems`, `radical.pilot`,
and users will interact to prepare, verify, and specify such software
environments before or during run time.

.. seealso:: Provisioning the SCALE-MS task environment.
    `Issue #141 <https://github.com/SCALE-MS/scale-ms/issues/141>`__.

Pure Python execution
=====================

For some use cases (such as Jupyter notebooks),
it may be preferable to configure the execution target and launch a workflow
entirely from within Python.

Such use cases are not yet well-supported in `scalems`.

Refer to the
`test suite <https://github.com/SCALE-MS/scale-ms/tree/master/tests>`__
for examples, or follow https://github.com/SCALE-MS/scale-ms/issues/82.
