=========================
scalems.radical execution
=========================

User interface is documented at :py:mod:`scalems.radical`.

execution module interface
--------------------------

.. autofunction:: scalems.radical.configuration

.. autofunction:: scalems.radical.workflow_manager

scalems.radical.runtime support module
--------------------------------------

.. automodule:: scalems.radical.runtime

.. autofunction:: scalems.radical.runtime.executor_factory

.. autoclass:: scalems.radical.runtime.RPDispatchingExecutor
    :members: runtime, runtime_configuration, runtime_startup, runtime_shutdown
    :member-order: bysource
    :exclude-members: __new__

.. autoclass:: scalems.radical.runtime.Configuration
    :undoc-members:
    :members:

.. autoclass:: scalems.radical.runtime.Runtime
    :members:

.. autofunction:: scalems.radical.runtime.rp_task

.. autofunction:: scalems.radical.runtime.scalems_callback

.. autofunction:: scalems.radical.runtime.submit

scalems.radical.raptor
----------------------

.. automodule:: scalems.radical.raptor
    :no-members:

master task
~~~~~~~~~~~

`scalems` specialization of the "master" component in the
:py:mod:`radical.pilot.raptor` federated scheduling protocol.

.. autofunction:: master

.. autofunction:: master_script

.. autofunction:: master_input

.. autofunction:: worker_requirements

.. autoclass:: MasterTaskConfiguration
    :members:

.. autoclass:: ClientWorkerRequirements
    :members:

.. autoclass:: ScaleMSMaster
    :members:

worker task
~~~~~~~~~~~

`scalems` specialization of the "worker" component in the
:py:mod:`radical.pilot.raptor` federated scheduling protocol.

.. autofunction:: worker_description

.. autoclass:: ScaleMSWorker
    :members:

task handling
~~~~~~~~~~~~~

`scalems` instructions are embedded in TaskDescriptions with the
:py:data:`scalems.radical.raptor.CPI_MESSAGE` mode,
using the :py:attr:`~radical.pilot.TaskDescription.metadata` field for the payload.
Executable work is re-encoded by `ScaleMSMaster` for `ScaleMSWorker`
(using a `ScalemsRaptorWorkItem` in the *work_item* field of the *kwargs* of
a ``TASK_FUNCTION`` mode Task)
to be dispatched through `scalems` machinery in the Worker process.

.. autodata:: api_name
    :annotation:

.. autodata:: CPI_MESSAGE
    :annotation:

.. autoclass:: ScalemsRaptorWorkItem
    :members:

..
    .. autofunction:: object_encoder

    .. autofunction:: raptor_work_deserializer

compatibility helpers
~~~~~~~~~~~~~~~~~~~~~

These classes are not formal types, but are used to represent (untyped)
interfaces in :py:mod:`radical.pilot.raptor`.

.. autoclass:: RaptorWorkerConfig
    :members:
    :exclude-members: __new__

.. autoclass:: WorkerDescription
    :members:
    :exclude-members: __new__

.. autoclass:: TaskDictionary
    :members:

.. autoclass::  _RaptorTaskDescription
    :members:
