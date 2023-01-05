"""Python logging facilities use the built-in logging module.

Upon import, the scalems package sets a placeholder "NullHandler" to block
propagation of log messages to the `handler of last resort
<https://docs.python.org/3/howto/logging.html#what-happens-if-no-configuration-is-provided>`__
(and to `sys.stderr`).

If you want to see logging output on `sys.stderr`, attach a
`logging.StreamHandler` to the 'scalems' logger.

Example::

    character_stream = logging.StreamHandler()
    # Optional: Set log level.
    logging.getLogger('scalems').setLevel(logging.DEBUG)
    character_stream.setLevel(logging.DEBUG)
    # Optional: create formatter and add to character stream handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    character_stream.setFormatter(formatter)
    # add handler to logger
    logging.getLogger('scalems').addHandler(character_stream)

To handle log messages that are issued while importing :py:mod:`scalems` and its submodules,
attach the handler before importing :py:mod:`scalems`. Note that if :py:mod:`scalems.radical`
will be used, you should import :py:mod:`radical.pilot` before importing `logging` to avoid spurious warnings.

Refer to submodule documentation for hierarchical loggers to allow
granular control of log handling (e.g. ``logging.getLogger('scalems.radical')``).
Refer to the Python :py:mod:`logging` module for information on connecting to and handling
logger output.
"""

__all__ = ["logger"]

# Import system facilities
from logging import DEBUG
from logging import getLogger
from logging import NullHandler

# Define `logger` attribute that is used by submodules to create sub-loggers.
logger = getLogger("scalems")
# By default, prevent scalems logs from propagating to the root logger (and to sys.stderr)
# if the user does not take action to handle logging.
logger.addHandler(NullHandler(level=DEBUG))
