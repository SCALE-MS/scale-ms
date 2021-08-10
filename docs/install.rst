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
[RADICAL Pilot installation](https://radicalpilot.readthedocs.io/en/stable/installation.html)

.. note:: Due to the way it is packaged, RP does not behave properly if its virtual environment is not explicitly activated.
    You must ``. /path/to/venv/bin/activate`` before installing or using ``radical.pilot`` or ``scalems.radical``.
    (It is insufficient simply to use ``/path/to/venv/bin/python -m scalems.radical ...``)

Configuration
-------------

Many of the configurable aspects of :py:mod:`scalems.radical` only allow you to refer to
resources prepared ahead of time in the filesystem.

Resource
~~~~~~~~

RP describes its targeted execution environment as a
`resource <https://radicalpilot.readthedocs.io/en/stable/machconf.html>`__.
Built-in resource definitions are provided with the :py:mod:`radical.pilot` package.
To extend or override the built-in resource definitions,
you must add or edit a resource
`configuration file <https://radicalpilot.readthedocs.io/en/stable/machconf.html#writing-a-custom-resource-configuration-file>`__
in your home directory **before launching :py:mod:`scalems.radical`**

.. note:: Password-less ssh private key is not necessary.
    It may not be clearly documented, but RP does not require that you set up a password-less ssh key pair.
    It is only necessary that RP is able to make new ssh connections at run time without storing or asking for a password.
    Refer to the ``ssh-agent`` documentation for your SSH client.

Notes
-----

RP creates many processes, threads, and files at run time.
Due to its loosely coupled, asynchronous architecture,
resources may not be released immediately when RP components shut down.
You may have to increase your ``ulimit`` allowances,
and it may not be possible to rapidly create and destroy execution sessions,
especially within a single process.

.. _virtual environment: https://docs.python.org/3/library/venv.html
