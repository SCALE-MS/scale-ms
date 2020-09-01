# Import radical.pilot early because of interaction with the built-in logging module.
# TODO: Did this work?
try:
    import radical.pilot
except ImportError:
    # It is not an error to run tests without RP, but when RP is available, we
    # need to import it before pytest imports the logging module.
    ...
