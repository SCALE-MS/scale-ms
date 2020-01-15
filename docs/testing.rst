=======
Testing
=======

Describe testing categories and tools.

Python test scripts in the repository :file:`tests/` directory are run with
:py:mod:`pytest`::

    python -m pytest tests/

Unit tests, system tests, and integration tests
===============================================

Describe the scopes of software component and interface testing,
testing policies, infrastructure.

Acceptance tests
================

.. note::

    It is probably easiest to initially describe and test some new functionality
    in single files (as literate code, Python docstrings, or Jupyter notebooks).
    We could curate documentation extracted from test files to generate
    content in this section, at least as a transitional way to publish as much
    as possible about experimental features or use cases, but that would require
    either packaging the tests in some way or at least manipulating the
    PYTHONPATH and making them ``import``-able.

Supporting code
===============

.. automodule:: serialization
    :members:
