"""Local execution dispatching for ScaleMS workflows.

Usage:
    python3 -m scalems.radical my_workflow.py

"""

# Note that we want to make sure that the asyncio event loop is started in the
# root thread before any RP Session is created.
