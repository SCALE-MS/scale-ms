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

        self.register_call('hello', self.hello)


    # --------------------------------------------------------------------------
    #
    def hello(self, count, uid):
        '''
        important work
        '''

        self._prof.prof('hello_start', uid=uid)

        out = 'hello %5d @ %.2f [%s]' % (count, time.time(), self._uid)
      # time.sleep(0.1)

        self._prof.prof('hello_io_start', uid=uid)
        self._log.debug(out)
        self._prof.prof('hello_io_stop', uid=uid)

        self._prof.prof('hello_stop', uid=uid)
        return out


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    worker = ScaleMSWorker(sys.argv[1])
    worker.run()


# ------------------------------------------------------------------------------

