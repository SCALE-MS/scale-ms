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

.. _rp setup:

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

    Some packages (including LAMMPS and RP) do not behave properly
    unless their virtual environment is explicitly activated.
    You must ``. /path/to/venv/bin/activate`` before installing or using
    :py:mod:`radical.pilot` or :py:mod:`scalems.radical`.
    (It is insufficient simply to use
    ``/path/to/venv/bin/python -m scalems.radical ...``)

Depending on various configuration information and workflow details,
RP automatically manages one or more venvs for Pilot agents and Tasks.
See :ref:`venvs` for details.

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

    RP documentation for `machconf` implies a need to set up a password-less
    ssh key pair, but this is not strictly necessary.
    It is only necessary that RP is able to make new ssh connections at run time
    without storing or asking for a password.
    Refer to the :program:`ssh-agent` documentation for your SSH client, and
    unlock your private key before launching your script with `scalems.radical`.

Setting resource parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~

https://radicalpilot.readthedocs.io/en/stable/machconf.html#writing-a-custom-resource-configuration-file
describes the user files for defining new resources or replacing built-in resource definitions.

To override the default logic for a built-in resource definition,
copy the JSON object for the resource(s) from your RP version
(e.g. https://github.com/radical-cybertools/radical.pilot/tree/devel/src/radical/pilot/configs)
to your home directory and then apply updates.

To update parameters for ``local.localhost``::

    mkdir $HOME/.radical/pilot/configs/
    cp $VIRTUAL_ENV/lib/python3*/site-packages/radical/pilot/configs/resource_local.json $HOME/.radical/pilot/configs/

Then edit the ``localhost`` JSON object in
:file:`$HOME/.radical/pilot/configs/resource_local.json`.

.. note::
    The resource definition determines the virtual environment in which
    remote Pilot agent software runs. See :ref:`venvs` for details.

Additional notes
----------------

RP creates many processes, threads, and files at run time.
Due to its loosely coupled, asynchronous architecture,
resources may not be released immediately when RP components shut down.
You may have to increase your ``ulimit`` allowances,
and it may not be possible to rapidly create and destroy execution sessions,
especially within a single process.

.. _virtual environment: https://docs.python.org/3/library/venv.html
