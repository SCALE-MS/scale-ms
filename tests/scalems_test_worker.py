#!/usr/bin/env python3

import sys
import time

import radical.pilot as rp
import radical.pilot.task_overlay as rpt


# ------------------------------------------------------------------------------
#
class ScaleMSWorker(rpt.Worker):


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rp.task_overlay.Worker.__init__(self, cfg)

        self.register_mode('gmx',   self._gmx)
      # self.register_call('hello', self.hello)


    # --------------------------------------------------------------------------
    #
    def _gmx(self, data):

        out = 'gmx  : %s %s' % (time.time(), data['blob'])
        err = None
        ret = 0

        return out, err, ret


    # --------------------------------------------------------------------------
    #
    def hello(self, world):

        return 'call : %s %s' % (time.time(), world)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    worker = ScaleMSWorker(sys.argv[1])
    worker.run()


# ------------------------------------------------------------------------------

