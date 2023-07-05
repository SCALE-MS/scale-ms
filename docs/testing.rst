=======
Testing
=======

Python test scripts in the repository :file:`tests/` directory are run with
:py:mod:`pytest`.

Dependencies
============

RADICAL Pilot installation
--------------------------
RADICAL Pilot has some tricky interactions with Python environment virtualization
and manipulation that lead to some caveats.
* Do not try to use "editable" installs.
* Explicitly *activate* the virtual environment before running a script that uses RP.

:py:mod:`radical.pilot` will probably not find the data files or entry point scripts that
it relies on unless you first "source" the ``activate`` script for a venv containing
a regular :py:mod:`radical.pilot` installation.
Perform testing with the virtual environment active.
Do not try to
exercise the installation or virtual environment through other techniques, such
as invoking a Python venv by explicitly calling the sym-linked :file:`.../bin/python`.

pytest
------
We use a few pytest plugins. To install them first, it should be sufficient to
use the :file:`requirements-testing.txt` file in the root of the repository.
::

    pip install -r requirements-testing.txt

Test data
=========

Some test scripts or examples rely on data that lives in a separate
`testdata <https://github.com/SCALE-MS/testdata>`__ repository.
This repository is noted as a
`git submodule <https://www.atlassian.com/git/tutorials/git-submodule>`__
in the :file:`.gitmodules` configuration file. GitHub respects and records
the repository link, so the testdata repository will be cloned with
https://github.com/SCALE-MS/scale-ms if you used the ``--recursive`` option
to :command:`git clone`. Otherwise, you will have to manually initialize
and update the submodule or otherwise acquire the test data.

Unit tests, system tests, and integration tests
===============================================

The test suite in :file:`tests/` includes a lot of integration tests that probe
interactions with full RADICAL Pilot sessions because we have had problems
reproducing viable execution environments and because we use RP features and
interfaces that may not be mature.

Refer to the :file:`docker/README.md` in the repository for more about
reproducible testing environments.

We install several pytest markers and command line options for the
tests in :file:`tests/`. Refer to :file:`tests/conftest.py` or use
:command:`pytest tests --help` (look for the ``custom options:`` section).

A typical invocation of the test suites in :file:`tests/`
 (including Python interpreter options, pytest options, and options specific to the scalems test fixtures)
 frequently looks something like the followin::
    python -X dev -m pytest -x -rA -l --log-cli-level debug tests/ --rp-venv $VIRTUAL_ENV --rp-resource docker.login --rp-access ssh

Acceptance tests
================

The automated GitHub Actions test pipeline includes command line invocations of
example scripts. We should continue to run short examples to ensure that the
scripting interface behaves as expected.

.. todo::

    It is probably easiest to initially describe and test some new functionality
    in single files (as literate code, Python docstrings, or Jupyter notebooks).
    We could curate documentation extracted from test files to generate
    content in this section, at least as a transitional way to publish as much
    as possible about experimental features or use cases, but that would require
    either packaging the tests in some way or at least manipulating the
    PYTHONPATH and making them ``import``-able.

Coverage
========

We use the Python `Coverage <https://coverage.readthedocs.io/>`__ package to
trace test coverage.
(For pytest tests, we use the `pytest-cov <https://pytest-cov.readthedocs.io/>`__
pytest plugin.)
In our GitHub Actions test pipeline, we gather coverage for both pytest suites
and command line examples, and upload the results to
`Codecov.io <https://app.codecov.io/gh/SCALE-MS/scale-ms>`__ for visualization
and for feedback on pull requests.

Aggregating coverage
--------------------

The ``--parallel-mode`` works pretty well to gather multiple data files, and
codecov.io does a good job of automatically merging multiple reports received
from a pipeline. We just have to make sure to use ``--append``
(or ``--cov-append``) appropriately for the data files, and to create appropriately
unique xml report files (for upload).

The default ``coverage`` behavior automatically follows threads, too.
However, for processes launched by RADICAL Pilot, we need to take extra steps
to run coverage and gather results.

Gathering remote coverage
-------------------------

When ``COVERAGE_RUN`` or ``SCALEMS_COVERAGE`` environment variables are detected,
:py:mod:`scalems.radical.runtime` modifies the Master TaskDescription to include
``python -m coverage run --data-file=coverage_dir/.coverage --parallel-mode ...``,
and adds an output staging directive to retrieve ``task:///coverage_dir``
to the predictably named directory ``./scalems-remote-coverage-dir``.
The ``--parallel-mode`` option makes sure that remotely generated master task
coverage data file will be uniquely named.

Note that :py:mod:`pytest-cov` does not set the ``COVERAGE_RUN`` environment
variable. When :command:`pytest --cov` is detected, we use a pytest fixture to
set ``SCALEMS_COVERAGE=TRUE`` in the testing process environment.

Even though the `ScaleMSRaptor.request_cb()` and `ScaleMSRaptor.result_cb()` are
called in separate threads spawned by RP, coverage should be correct.

We cannot customize the command line for launching the Worker task, so for
coverage of the Worker and its dispatched function calls, we need to use the
Coverage API.
These details are encapsulated in the
:py:func:`scalems.radical.raptor.coverage_file` decorator.
