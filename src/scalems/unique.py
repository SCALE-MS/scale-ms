"""Tools and hacks for generating unique identifiers."""
import contextvars


def next_monotonic_integer() -> int:
    """Utility for generating a monotonic sequence of integers throughout the interpreter.

    Not thread-safe. However, threads may
    * avoid race conditions by copying the contextvars context for non-root threads, or
    * reproduce the sequence of the main thread by calling this function an equal
      number of times.

    Returns:
        Next integer.

    """
    value = _monotonic_integer.get()
    _monotonic_integer.set(value + 1)
    return value


_monotonic_integer = contextvars.ContextVar("_monotonic_integer", default=0)
