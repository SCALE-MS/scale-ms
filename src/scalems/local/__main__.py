"""Local execution dispatching for ScaleMS workflows.

Usage:
    python3 -m scalems.local my_workflow.py

"""

import argparse
import sys

import scalems.invocation


if __name__ == '__main__':
    sys.exit(scalems.invocation.run(scalems.local.AsyncWorkflowManager))
