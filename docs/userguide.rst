==========
User Guide
==========

Getting Started
===============

The :py:mod:`scalems` package requires a supported Python3 installation
and assumes a Linux environment. For remote execution, SCALE-MS uses
RADICAL Pilot, which has additional requirements.

See :doc:`install`

Invocation
==========

.. automodule:: scalems.invocation

Idioms
======

Deferred execution
------------------

SCALE-MS allows the specific calculations in a workflow to be expressed
independently of its execution. Commands return handles to future results,
allowing chains of commands and data flow to be described before dispatching
for execution.

This programming model is consistent with modern concurrency idioms,
with an additional proxy layer that allows multiple tasks to be configured
before any are launched. Compared to the standard Python concurrency modules,
:py:mod:`asyncio` functionality that is only available within an ``async def``
function is available directly to the scripting interface, replacing ad hoc
coroutine definitions with objects (:term:`operation instance` s)

Parallel data flow
------------------

Generally, single instructions can be applied to multiple data without special
syntax.
An array of input streams implies an array of output streams.
All SCALE-MS objects have "shape" as part of their typing information,
and parallel streams of data may be represented by a single reference of
higher dimensionality.
Function inputs have specified typing, which allows the multiplicity of a
command to be inferred from its input.

By default, sequencing is preserved in outer dimensions.
In other words replicated pipelines can be consistently indexed.

Sometimes, bundles of data should be processed asynchronously and the unique
identity of the data source is less important. In such use cases, the sequenced
outer dimension can be explicitly converted to an asynchronous iterable.

Generally, commands that consume sequenced input produce sequenced output,
while commands provided with unsequenced / unordered / asynchronous input produce
unordered output.

Iteration
---------

Iteration in SCALE-MS takes a few different forms, and we should first clarify
a distinction between iterable objects and iterable coroutines.

As noted above, SCALE-MS data has shape. As with numpy, it is helpful to think
in terms of "vectorized" operations instead of explicitly looping over elements.
Most ``for`` or ``foreach`` use cases are handled implicitly by applying a
function to iterable inputs.
The functional style `scalems.map` can be used to apply a function
to the elements of an iterable.
This can be necessary when the operation instance needs to be generated
dynamically, such as when the shape of data is not known until run time.
It can also be useful to convert non-SCALE-MS functions or data into workflow
objects (to explicitly defer execution of functions implemented outside of the
data flow API).

Of course, some iteration is not vectorizable.
Logic may be explicitly stateful, or commands may hide internal data graph management.
The main looping construct in SCALE-MS, then, is `scalems.while_loop`.
The *condition* of the *while* loop is evaluated before each application of the
*function*.

Dynamic functions
-----------------

Simple SCALE-MS commands add :term:`operation instance` s to the work graph

:py:func:`scalems.map`

while_loop

conditional

Python interface
================

Data flow scripting interface is provided by the :py:mod:`scalems` Python package.

.. seealso:: :doc:`python`
