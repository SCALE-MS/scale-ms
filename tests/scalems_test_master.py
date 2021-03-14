#!/usr/bin/env python3

import sys

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
class ScaleMSMaster(rp.raptor.Master):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):
        rp.raptor.Master.__init__(self, cfg=cfg)

        self._log = ru.Logger(self.uid, ns='radical.pilot')

    # --------------------------------------------------------------------------
    #
    def result_cb(self, requests):
        for r in requests:
            r['task']['stdout'] = r['out']

            print('result_cb %s: %s [%s]' % (r.uid, r.state, r.result))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':
    cfg = ru.Config(cfg=ru.read_json(sys.argv[1]))
    master = ScaleMSMaster(cfg)

    master.submit(descr=cfg.worker_descr, count=cfg.n_workers,
                  cores=cfg.cpn, gpus=cfg.gpn)

    master.start()
    master.join()
    master.stop()

# ------------------------------------------------------------------------------
