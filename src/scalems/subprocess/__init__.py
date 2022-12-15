"""Define the ScaleMS Subprocess command.

scalems.executable() is used to execute a program in one (or more) subprocesses.
It is an alternative to the built-in Python subprocess.Popen or asyncio.create_subprocess_exec
with extensions to better support ScaleMS execution dispatching and ensemble data flow.

The core task is represented by ``scalems.subprocess.SubprocessTask``, but also
requires the definition of ``scalems.subprocess.SubprocessInput`` and
``scalems.subprocess.SubprocessResult``.



In the first iteration, we can use dataclasses.dataclass to define input/output data structures
in terms of standard types. In a follow-up, we can use a scalems metaclass to define them
in terms of Data Descriptors that support mixed scalems.Future and native constant data types.
"""

__all__ = ["executable", "Subprocess", "SubprocessInput", "SubprocessResult"]

import logging

from . import _subprocess

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

Subprocess = _subprocess.Subprocess
SubprocessInput = _subprocess.SubprocessInput
SubprocessResult = _subprocess.SubprocessResult

executable = _subprocess.executable
