=======
Testing
=======

Describe testing categories and tools.

Python test scripts in the repository :file:`tests/` directory are run with
:py:mod:`pytest`::

    python -m pytest tests/

We use a few pytest plugins. To install them first, it should be sufficient to
use the :file:`requirements-testing.txt` file in the root of the repository.
::

    pip install -r requirements-testing.txt

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

.. todo:: Describe the scopes of software component and interface testing,
    testing policies, infrastructure.

The test suite in :file:`tests/` includes a lot of integration tests that probe
interactions with full RADICAL Pilot sessions because we have had problems
reproducing viable execution environments and because we use RP features and
interfaces that may not be mature.

Refer to the :file:`docker/README.md` in the repository for more about
reproducible testing environments.

We install several pytest markers and command line options for the
tests in :file:`tests/`. Refer to :file:`tests/conftest.py` or use
:command:`pytest tests --help` (look for the ``custom options:`` section).

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
