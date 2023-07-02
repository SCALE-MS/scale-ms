from __future__ import annotations

from scalems.exceptions import ScaleMSError


class ScaleMSRadicalError(ScaleMSError):
    """Base exception for `scalems.radical` backend."""


class RPConfigurationError(ScaleMSRadicalError):
    """Unusable RADICAL Pilot configuration."""


class RPInternalError(ScaleMSError):
    """RADICAL Pilot is misbehaving, probably due to a bug.

    Please report the potential bug.
    """


class RPConnectionError(ScaleMSRadicalError):
    """A problem connecting to the RCT back end or remote resources."""
