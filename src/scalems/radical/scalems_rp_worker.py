"""Provide the entry point for SCALE-MS tasks dispatched by scalems_rp_agent."""


import sys
import time

import radical.pilot as rp
import radical.pilot.raptor as rpt

import logging

logger = logging.getLogger('scalems_rp_worker')


class ScaleMSWorker(rpt.Worker):
    def __init__(self, cfg):

        rp.raptor.Worker.__init__(self, cfg)

        self.register_mode('gmx',   self._gmx)

    def _gmx(self, data):

        out = 'gmx  : %s %s' % (time.time(), data['blob'])
        err = None
        ret = 0

        return out, err, ret

    def hello(self, world):

        return 'call : %s %s' % (time.time(), world)


def main():
    worker = ScaleMSWorker(sys.argv[1])
    worker.start()
    time.sleep(5)
    worker.join()


if __name__ == '__main__':
    # For additional console logging, create and attach a stream handler.
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)
    # Should we be interacting with the RP logger?

    sys.exit(main())
