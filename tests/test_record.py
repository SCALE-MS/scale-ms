"""Test the infrastructure for managing and manipulating the workflow record."""
import pytest

import scalems.workflow
from scalems.encoding import EncodedRecordDict


def test_basic_serialization(event_loop):
    """Construct a trivial workflow and test round trip through JSON."""
    manager = scalems.workflow.WorkflowManager(loop=event_loop,
                                               executor_factory=lambda: None)
    # TODO: Add some objects to the workflow.
    record: EncodedRecordDict = manager.encode()
    assert set(record.keys()) == {'version', 'types', 'items'}
    assert record['version'] == 'scalems_workflow_1'

    manager = scalems.workflow.WorkflowManager(loop=event_loop,
                                               executor_factory=lambda: None)
    manager.load(record)
    # TODO: Confirm equivalent contents.


@pytest.mark.skip('Unimplemented.')
def test_dag_topology():
    """Try to confirm that the topological sorting works.

    The workflow record must be in a topologically valid order when serialized.

    The workflow manager must detect topologically invalid workflow records by checking
    dependencies during deserialization.
    """
    # Test topologically valid output.
    ...
    # Test that topologically invalid input is detected.
    ...


@pytest.mark.skip('Unimplemented.')
def test_object_placement():
    """Test object insertion through the workflow manager interface.

    Use the workflow manager interface to place various object types into the workflow,
    confirming the registration schemes for data types.
    """
    # Test integer placement.
    # Test float placement.
    # Test bool placement.
    # Test string placement.
    # Test Path placement.
    # Test integer array placement.
    ...


@pytest.mark.skip('Unimplemented.')
def test_references():
    """Test reference resolution and validation.

    Add objects containing valid and invalid references. Make sure that the valid
    references resolve. Make sure that the invalid references are rejected.
    """
    ...
