"""Local execution dispatching for ScaleMS workflows.

Usage:
    python3 -m scalems.local my_workflow.py

"""

import sys

import scalems.invocation

if __name__ == "__main__":
    sys.exit(
        scalems.invocation.run(
            manager_factory=scalems.local.workflow_manager, executor_factory=scalems.local.executor_factory
        )
    )
