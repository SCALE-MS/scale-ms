"""Dispatch scalems.exec through RADICAL Pilot."""

import pytest

with_radical_only = None

try:
    import radical.pilot as rp
    import radical.utils as ru
except ImportError:
    rp = None
    ru = None

with_radical_only = pytest.mark.skipif(None in (rp, ru), reason="Test requires RADICAL stack.")


@with_radical_only
def test_rp_import():
    assert rp is not None
    assert ru is not None
