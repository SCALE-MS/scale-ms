"""Execution environment context management."""

__all__ = ("scoped_chdir",)

import contextlib
import logging
import os
import pathlib
import threading
import typing
import warnings

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

cwd_lock = threading.Lock()


@contextlib.contextmanager
def scoped_chdir(directory: typing.Union[str, bytes, os.PathLike]):
    """Restore original working directory when exiting the context manager.

    Caveats:
        Current working directory is a process-level property. To avoid unexpected
        behavior across threads, only one instance of this context manager may be
        active at a time. If necessary, we could allow for nested cwd contexts,
        but we cannot make this behavior thread-safe.

    See Also:
        https://github.com/SCALE-MS/scale-ms/issues/166
    """
    if isinstance(directory, bytes):
        directory = os.fsdecode(directory)
    path = pathlib.Path(directory)
    if not path.exists() or not path.is_dir():
        raise ValueError(f"Not a valid directory: {str(directory)}")
    if cwd_lock.locked():
        warnings.warn("Another call has already used scoped_chdir. Waiting for lock...")
    with cwd_lock:
        oldpath = os.getcwd()
        os.chdir(path)
        logger.debug(f"Changed current working directory to {path}")
        try:
            yield path
            # If the `with` block using scoped_chdir produces an exception, it will
            # be raised at this point in this function. We want the exception to
            # propagate out of the `with` block, but first we want to restore the
            # original working directory, so we skip `except` but provide a `finally`.
        finally:
            logger.debug(f"Changing working directory back to {oldpath}")
            os.chdir(oldpath)
    logger.info("scoped_chdir exited.")
