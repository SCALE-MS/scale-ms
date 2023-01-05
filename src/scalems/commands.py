"""Basic SCALE-MS Commands.

Core commands for building workflows.

The availability and behavior of these commands is part of the SCALE-MS
specification.
"""
# We expect these commands to be identified at the top level scalems namespace
# with e.g. ('scalems', 'logical_not'). We keep them in a submodule just
# to be tidy.

__all__ = [
    "desequence",
    "extend_sequence",
    "gather",
    "logical_and",
    "logical_not",
    "map",
    "reduce",
    "resequence",
    "scatter",
    "subgraph",
    "while_loop",
]

import typing

from scalems import exceptions


def subgraph(variables=None):
    """Allow operations to be configured in a sub-context.

    The object returned functions as a Python context manager. When entering the
    context manager (the beginning of the ``with`` block), the object has an
    attribute for each of the named ``variables``. Reading from these variables
    gets a proxy for the initial value or its update from a previous loop iteration.
    At the end of the ``with`` block, any values or data flows assigned to these
    attributes become the output for an iteration.

    After leaving the ``with`` block, the variables are no longer assignable, but
    can be called as bound methods to get the current value of a variable.

    When the object is run, operations bound to the variables are ``reset`` and
    run to update the variables.

    Example::

        @scalems.function_wrapper(output={'data': float})
        def add_float(a: float, b: float) -> float:
            return a + b

        @scalems.function_wrapper(output={'data': bool})
        def less_than(lhs: float, rhs: float, output=None):
            output.data = lhs < rhs

        subgraph = scalems.subgraph(variables={'float_with_default': 1.0, 'bool_data': True})
        with subgraph:
            # Define the update for float_with_default to come from an add_float operation.
            subgraph.float_with_default = add_float(subgraph.float_with_default, 1.).output.data
            subgraph.bool_data = less_than(lhs=subgraph.float_with_default, rhs=6.).output.data
        operation_instance = subgraph()
        operation_instance.run()
        assert operation_instance.values['float_with_default'] == 2.

        loop = scalems.while_loop(function=subgraph, condition=subgraph.bool_data)
        handle = loop()
        assert handle.output.float_with_default.result() == 6

    """
    raise exceptions.MissingImplementationError()


def logical_not(value):
    """Negate boolean inputs."""
    raise exceptions.MissingImplementationError()


def logical_and(iterable):
    """Produce a boolean value resulting from the logical AND of the elements of the input.

    The API does not specify whether the result is published before all values
    of *iterable* have been inspected, but (for checkpointing, reproducibility,
    and simplicity of implementation), the operation is not marked "complete"
    until all inputs have been resolved.
    """
    raise exceptions.MissingImplementationError()


def while_loop(*, function, condition, max_iteration=10, **kwargs):
    """Generate and run a chain of operations such that condition evaluates True.

    Returns a reference that acts like a single operation in the current
    work graph, but which is a proxy to the operation at the end of a dynamically generated chain
    of operations. At run time, *condition* is evaluated for the last element in
    the current chain. If *condition* evaluates False, the chain is extended and
    the next element is executed. When *condition* evaluates True, the object
    returned by `while_loop` becomes a proxy for the last element in the chain.

    TODO: Allow external access to intermediate results / iterations?

    Arguments:
        function: a callable that produces an instance of an operation when called (with ``**kwargs``, if provided).
        condition: a call-back that returns a boolean when provided an operation instance.
        max_iteration: execute the loop no more than this many times (default 10)

    *function* (and/or *condition*) may be a "partial function" and/or stateful function object
    (such as is produced by subgraph()) in order to retain state between iterations.

    TODO: Specify requirements of *function* and *condition*. (Picklable? scalems.Function compatible?)

    TODO: Describe how *condition* could be fed asynchronously from outside of the wrapped *function*.

    TODO: Does *condition* need to evaluate directly to a native bool, or do we
        detect whether a Future is produced, and call ``result()`` if necessary?

    Warning:
        *max_iteration* is provided in part to minimize the cost of bugs in early
        versions of this software. The default value may be changed or
        removed on short notice.

    """
    raise exceptions.MissingImplementationError()


def desequence(iterable):
    """Remove sequencing from an iterable.

    Given an input of shape (N, [M, ...]), produce an iterable of resources
    (of shape (M,...)). Ordering information from the outer dimension is lost,
    even in an ensemble scope, in which parallel data flows generally retain
    their ordering.

    This allows iterative tools (e.g. `map`) to use unordered or asynchronous
    iteration on resource slices as they become available.
    """
    raise exceptions.MissingImplementationError()


def resequence(keys, collection):
    """Set the order of a collection.

    Use the ordered list of keys to define the sequence of the elements in the
    collection.

    In addition to reordering an ordered collection, this function is useful
    for applying a sequence from one part of a work flow to data that has been
    processed asynchronously.
    """
    raise exceptions.MissingImplementationError()


def gather(iterable):
    """Convert an iterable or decomposable collection to a complete collection.

    Use to synchronize/localize data. Reference with ambiguous dimensionality or
    decomposable dimensions are converted to references with concrete dimensionality
    and non-decomposable dimensions. When the output of *gather* is used as input
    to another function, the entire results of *iterable* are available in the
    operation context.

    This function should generally not be necessary when defining a work flow,
    but may be necessary to disambiguate data flow topology, such as when N
    operations should each consume an N-dimensional resource in its entirety,
    in which case, *gather* is implicitly an *all_gather*.

    Alternatives:
        For decomposable dimensions, we may want to allow a named *scope* for
        the decomposition. Then, explicit *gather* use cases would be replaced
        by an annotation to change the *scope* for a dimension, and, as part of
        a function implementation's assertion of its decomposability, a function
        could express its ensemble/decomposition behavior in terms of a particular
        target scope of an operation.
    """
    raise exceptions.MissingImplementationError()


def scatter(iterable, axis=1):
    """Explicitly decompose data.

    For input with outer dimension size N and dimensionality D,
    or for an iterable of length N and elements with dimensionality D-1,
    produce a Resource whose outer dimensions are decomposed down to dimension
    D-*axis*.

    Note: This function is not necessary if we either require fixed dimensionality
    of function inputs or minimize implicit broadcast, scatter, and gather behavior.
    Otherwise, we will need to disambiguate decomposition.
    """
    raise exceptions.MissingImplementationError()


def reduce(function, iterable):
    """Repeatedly apply a function.

    For an Iterable[T] and a function that maps (T, T) -> T, apply the function
    to map Iterable[T] -> T. Assumes *function* is associative.

    If *iterable* is ordered, commutation of operands is preserved,
    but associativity of the generated operations is not specified.
    If *iterable* is unordered, the caller is responsible for ensuring that
    *function* obeys the commutative property.

    Compare to :py:func:`functools.reduce`
    """
    raise exceptions.MissingImplementationError()


def extend_sequence(sequence_a, sequence_b):
    """Combine sequential data into a new sequence."""
    raise exceptions.MissingImplementationError()


def map(function, iterable, shape=None):
    """Generate a collection of operations by iteration.

    Apply *function* to each element of *iterable*.

    If *iterable* is ordered, the generated operation collection is ordered.

    If *iterable* is unordered, the generated operation collection is unordered.
    """
    raise exceptions.MissingImplementationError()


class Callable(typing.Protocol):
    """This protocol describes the required function signature for a SCALE-MS command."""

    def __call__(self):
        ...


class Command(typing.Protocol):
    """Protocol describing a SCALE-MS Command."""


def command(obj: Callable) -> Command:
    """Decorate a callable to create a SCALE-MS Command."""
    raise exceptions.MissingImplementationError


def function_wrapper(output: dict = None):
    # Suppress warnings in the example code.
    # noinspection PyUnresolvedReferences
    """Generate a decorator for wrapped functions with signature manipulation.

    New function accepts the same arguments, with additional arguments required by
    the API.

    The new function returns an object with an ``output`` attribute containing the
    named outputs.

    Example:

        >>> @function_wrapper(output={'spam': str, 'foo': str})
        ... def myfunc(parameter: str = None, output=None):
        ...    output.spam = parameter
        ...    output.foo = parameter + ' ' + parameter
        ...
        >>> operation1 = myfunc(parameter='spam spam')
        >>> assert operation1.spam.result() == 'spam spam'
        >>> assert operation1.foo.result() == 'spam spam spam spam'

    Arguments:
        output (dict): output names and types

    If ``output`` is provided to the wrapper, a data structure will be passed to
    the wrapped functions with the named attributes so that the function can easily
    publish multiple named results. Otherwise, the ``output`` of the generated operation
    will just capture the return value of the wrapped function.
    """
    raise exceptions.MissingImplementationError()


def poll():
    """Inspect the execution status of an operation.

    Inspects the execution graph state in the current context at the time of
    execution.

    Used in a work graph, this adds a non-deterministic aspect, but adds truly
    asynchronous adaptability.
    """
    raise exceptions.MissingImplementationError()
