import json
import os


class Encoder(json.JSONEncoder):
    """Extend the JSONEncoder for representations in the SCALE-MS data model."""
    def default(self, o):
        if isinstance(o, bytes):
            # TODO: use stronger check for UID, or bytes-based objects.
            return o.hex()
        if isinstance(o, os.PathLike):
            return os.fsdecode(o)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, o)
