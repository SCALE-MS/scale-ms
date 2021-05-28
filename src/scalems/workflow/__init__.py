"""WorkflowManager and Item interfaces.

"Workflow Manager" is an important parameter in several cases.
Execution management tools like scalems.run(), scalems.wait(), and scalems.dispatch()
update the workflow managed in a particular scope, possibly by interacting with
other scopes. Commands for declaring work or data add items to specific instances
of workflow managers. Internally, SCALEMS explicitly refers to theses scopes as
*manager*, or *context*, depending on the nature of the code collaboration.
*manager* may appear as an optional parameter for user-facing functions to allow
a particular managed workflow to be specified.
"""
__all__ = [
    'Description',
    'ItemView',
    'Task',
    'WorkflowManager',
    'workflow_item_director_factory',
]

import logging

from ._manager import Description
from ._manager import ItemView
from ._manager import Task
from ._manager import workflow_item_director_factory
from ._manager import WorkflowManager

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

# Future interface:
# class WorkflowEditor(abc.ABC):
#     """Allow infrastructure to update workflow object state."""
#     @abc.abstractmethod
#     def edit_item(self, item) -> Task:
#         ...
