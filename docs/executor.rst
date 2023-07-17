==========================
Execution middleware layer
==========================

Executable graphs or graph segments produced by client software are dispatched
and translated for execution on managed computing resources.

scalems.execution
=================

.. automodule:: scalems.execution

.. autoclass:: AbstractWorkflowUpdater
    :members:

.. autoclass:: RuntimeManager
    :members:

.. autofunction:: dispatch

.. autofunction:: manage_execution

scalems.workflow
================

.. automodule:: scalems.workflow
    :members:

scalems.context
===============

.. automodule:: scalems.context
    :members:

scalems.messages
================

.. automodule:: scalems.messages
    :members:

Backends
========

See :doc:`invocation` for user facing module interfaces.

Built-in Execution Modules include `scalems.radical`.

For command line usage, an `backend` should support interaction with the
`scalems.invocation` module.

At least temporarily, we also have a non-normative internal Execution Module
for executing serialized function calls as shell command lines.

scalems.call
------------

.. automodule:: scalems.call
    :members:

Additional internal details
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: function_call_to_subprocess

.. autoclass:: scalems.call._Subprocess
    :members:

Collaborations
~~~~~~~~~~~~~~

.. autofunction:: scalems.radical.task.subprocess_to_rp_task

.. autofunction:: scalems.radical.task.wrapped_function_result_from_rp_task

.. autoclass:: scalems.radical.task.RPTaskResult
    :members:

Support for execution module authors: :py:mod:`scalems.invocation`
==================================================================

.. automodule:: scalems.invocation

.. autofunction:: scalems.invocation.run

utilities
---------

Execution module authors should also be aware of the following utilities.

.. autofunction:: scalems.invocation.base_parser

.. autofunction:: scalems.invocation.make_parser
