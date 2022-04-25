==============================
Installation and configuration
==============================

Basic
=====

:py:mod:`scalems` requires Python 3.8 or higher.
We recommend setting up your Python for each project in a separate `virtual environment`_.

The :py:mod:`scalems` package can then be installed from the root directory
of a local copy of the SCALE-MS software repository.

Example:

.. code-block:: shell

    $ python3 -m venv myproject
    $ . myproject/bin/activate
    (myproject) $ git clone https://github.com/SCALE-MS/scale-ms.git
    (myproject) $ cd scale-ms
    (myproject) $ python -m pip install --upgrade pip setuptools
    (myproject) $ pip install .

For a more complete set of Python packages,
and for the exact versions of dependencies currently used for development,
use the :file:`requirements-testing.txt` file in the root directory of the repository::

    pip install -r requirements-testing.txt

RADICAL Pilot (RP)
==================

Execution through the :py:mod:`scalems.radical` backend requires additional set up.

You must have a functioning
`RADICAL Pilot (RP) installation <https://radicalpilot.readthedocs.io/en/stable/installation.html>`__.

Python virtual environment
--------------------------

Create a new Python virtual environment. (See, for example, :py:mod:`venv`.)
Activate the environment, then install `scalems` (and the RP software),
along with any software required for your workflow.

.. admonition:: Explicitly activate the virtual environment.

    Some packages (including LAMMPS and RP) do not behave properly unless their virtual environment is explicitly activated.
    You must ``. /path/to/venv/bin/activate`` before installing or using ``radical.pilot`` or ``scalems.radical``.
    (It is insufficient simply to use ``/path/to/venv/bin/python -m scalems.radical ...``)

You will need an equivalent virtual environment in the execution environment.
If you are using a shared filesystem
(or if you are using the *local.localhost* `RP resource`_
or *local* :py:data:`~radical.pilot.PilotDescription.access_schema`)
then you can execute in the same venv used on the client side.
Otherwise, you will need to prepare a virtual environment
(accessible to the chosen `RP resource`_) and inform `scalems.radical` of it
at run time. (See :option:`scalems.radical --venv`)

When executing the workflow, `scalems.radical` will automatically direct RP to *activate*
the chosen virtual environment before launching Tasks.

Configuration
-------------

Many of the configurable aspects of :py:mod:`scalems.radical` only allow you to refer to
resources prepared ahead of time in the filesystem.

.. _RP resource:

Resource
~~~~~~~~

When used in discussion of the :py:mod:`scalems.radical` module,
the term *resource* refers to the string name of an existing
`RP resource definition <https://radicalpilot.readthedocs.io/en/stable/machconf.html>`__

RP describes its targeted execution environment as a
`resource <https://radicalpilot.readthedocs.io/en/stable/machconf.html>`__ definition.
Built-in resource definitions are provided with the :py:mod:`radical.pilot` package.
To extend or override the built-in resource definitions,
you must add or edit a resource
`configuration file <https://radicalpilot.readthedocs.io/en/stable/machconf.html#writing-a-custom-resource-configuration-file>`__
in your home directory **before launching** :py:mod:`scalems.radical`.

.. note:: Password-less ssh private key is not necessary.
    It may not be clearly documented, but RP does not require that you set up a password-less ssh key pair.
    It is only necessary that RP is able to make new ssh connections at run time without storing or asking for a password.
    Refer to the ``ssh-agent`` documentation for your SSH client.

Setting resource parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~

https://radicalpilot.readthedocs.io/en/stable/machconf.html#writing-a-custom-resource-configuration-file
describes the user files for defining new resources or replacing built-in resource definitions.

To override the default logic for a built-in resource definition,
copy the JSON object for the resource(s) from your RP version
(e.g. https://github.com/radical-cybertools/radical.pilot/tree/devel/src/radical/pilot/configs)
to your home directory and then apply updates.

For example
"""""""""""

To update parameters for ``local.localhost``::

    mkdir $HOME/.radical/pilot/configs/
    cp $VIRTUAL_ENV/lib/python3*/site-packages/radical/pilot/configs/resource_local.json $HOME/.radical/pilot/configs/

Then edit the ``localhost`` JSON object in :file:`$HOME/.radical/pilot/configs/resource_local.json`.

More notes on Python virtual environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pilot environment
~~~~~~~~~~~~~~~~~

The RADICAL Pilot remote software components are based in a Python virtual environment
determined by parameters in the
`resource <https://radicalpilot.readthedocs.io/en/stable/machconf.html>`__ definition.

By default, RADICAL Pilot resources are configured to bootstrap the target environment
by creating a fresh virtual environment. (``virtenv_mode=create`` and ``rp_version=local``
in most `resource <https://radicalpilot.readthedocs.io/en/stable/machconf.html>`__ definitions.)
``virtenv_mode=update`` is a better choice than ``create``, so that later sessions can re-use a previously bootstrapped pilot venv.

To minimize the amount of bootstrapping RP performs for each :py:class:`~radical.pilot.Session`,
you can set up a completely static set of virtual environments with customized resource definitions
in :file:`$HOME/.radical/pilot/configs/`.
Configure the `RP resource`_ to *use* an existing *virtenv* and the RP installation it contains.
Set ``virtenv_mode=use``, ``virtenv=/path/to/venv``, ``rp_version=installed`` in the RP resource
definition.

.. note:: This optimization is relevant even for the ``local.localhost`` resource and ``local`` access scheme!

The user (or client) is
then responsible for maintaining venv(s) with the correct RCT stack (matching the API
used by the client-side RCT stack), the `scalems` package, and any dependencies of the
workflow.

Task environment
~~~~~~~~~~~~~~~~

In addition to :py:attr:`~radical.pilot.TaskDescription.pre_exec`, TaskDescriptions have
:py:attr:`~radical.pilot.TaskDescription.pre_launch`
and :py:attr:`~radical.pilot.TaskDescription.pre_rank`
hooks to set up the task environment.
Note that, in addition to the attribute descriptions,
there is additional discussion at the bottom of the
:py:class:`~radical.pilot.TaskDescription` class documentation section.

RADICAL Pilot now allows a :py:class:`~radical.pilot.Task` some explicitly Python-aware
environment preparation, (though users are still free to activate Task venvs using
:py:data:`~radical.pilot.TaskDescription.pre_exec`).
TaskDescription may use :py:attr:`~radical.pilot.TaskDescription.named_env` to identify
a virtual environment to be activated for the Task.
The virtual environment may be an existing virtual environment, or a new environment,
scheduled for creation with :py:func:`~radical.pilot.Pilot.prepare_env`.

In addition to supporting :py:attr:`~radical.pilot.TaskDescription.named_env` and the
other task environment hooks,
:py:class:`~radical.pilot.raptor.Master` and :py:class:`~radical.pilot.raptor.Worker`
tasks have some of the RP stack injected into their environment.
Raptor tasks launched through a Worker are executed in new processes
that are launched by the Worker through various mechanisms, depending
on the task requirements (various possible launch methods), including
possibly being forked from the Worker interpreter process.
:py:mod:`scalems.radical` dispatches (most) tasks through
raptor "call" mode, so it constructs and uses a venv for the Worker,
but **must** not specify ``named_env`` for work load tasks.

Additional notes
----------------

RP creates many processes, threads, and files at run time.
Due to its loosely coupled, asynchronous architecture,
resources may not be released immediately when RP components shut down.
You may have to increase your ``ulimit`` allowances,
and it may not be possible to rapidly create and destroy execution sessions,
especially within a single process.

.. _virtual environment: https://docs.python.org/3/library/venv.html
