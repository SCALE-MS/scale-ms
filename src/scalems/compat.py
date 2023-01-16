"""Compatibility helpers."""
import asyncio
import contextvars
import functools
import typing

# TODO(Python 3.9): Use functools.cache instead of lru_cache when Py 3.9 is required.
_cache = getattr(functools, "cache", functools.lru_cache(maxsize=None))
_ReturnT = typing.TypeVar("_ReturnT")


class FuncToThread(typing.Protocol[_ReturnT]):
    """Function object type of the asyncio.to_thread callable.

    typing.Protocol supports better checking of function signatures than Callable,
    especially with respect to ``*args`` and ``**kwargs``.
    """

    def __call__(
        self, __func: typing.Callable[..., _ReturnT], *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Coroutine[typing.Any, typing.Any, _ReturnT]:
        ...


@_cache
def get_to_thread() -> FuncToThread:
    """Provide a to_thread function.

    asyncio.to_thread() appears in Python 3.9, but we only require 3.8 as of this writing.
    """
    try:
        from asyncio import to_thread as _to_thread
    except ImportError:

        async def _to_thread(
            __func: typing.Callable[..., _ReturnT], *args: typing.Any, **kwargs: typing.Any
        ) -> _ReturnT:
            """Port Python asyncio.to_thread for Py 3.8."""
            context = contextvars.copy_context()
            wrapped_function: typing.Callable[[], _ReturnT] = functools.partial(context.run, __func, *args, **kwargs)
            assert callable(wrapped_function)
            loop = asyncio.get_running_loop()
            coro: typing.Awaitable[_ReturnT] = loop.run_in_executor(None, wrapped_function)
            result = await coro
            return result

    return _to_thread
