"""Execution dispatched through RADICAL Pilot for ScaleMS workflows.

Usage:
    python3 -m scalems.radical <RP options> my_workflow.py <script options>

"""

import sys

import scalems.invocation

# Can we attach to the rp Logger here?

if __name__ == "__main__":
    sys.exit(
        scalems.invocation.run(
            manager_factory=scalems.radical.workflow_manager, executor_factory=scalems.radical.executor_factory
        )
    )
