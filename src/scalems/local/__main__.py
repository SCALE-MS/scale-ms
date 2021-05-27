"""Local execution dispatching for ScaleMS workflows.

Usage:
    python3 -m scalems.local my_workflow.py

"""

import sys

import scalems.invocation
import scalems.local

if __name__ == '__main__':
    sys.exit(scalems.invocation.run(backend=scalems.local))
