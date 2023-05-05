"""Entry point for the Master Task role of the scalems.radical.raptor module.

An alternative to the setuptools generated entry-point script (:file:`scalems_rp_master`).
"""

import sys

if __name__ == "__main__":
    import logging

    scalems_logger = logging.getLogger("scalems")

    # TODO: Configurable log level?
    scalems_logger.setLevel(logging.DEBUG)
    character_stream = logging.StreamHandler()
    character_stream.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    character_stream.setFormatter(formatter)
    scalems_logger.addHandler(character_stream)

    file_logger = logging.FileHandler("scalems.radical.raptor.log")
    file_logger.setLevel(logging.DEBUG)
    file_logger.setFormatter(formatter)
    logging.getLogger("scalems").addHandler(file_logger)

    from scalems.radical.raptor import raptor

    sys.exit(raptor())
