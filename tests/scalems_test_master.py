#!/usr/bin/env python3

import os
import sys
import glob
import time

import threading     as mt

import radical.utils as ru
import radical.pilot as rp


# This script has to run as a task within an pilot allocation.  It will
#
#   - start scalems_workers on all nodes;
#   - watch the staging directory (`./scalems_new`) for incoming work;
#   - dispatch the tasks from those work descriptions to the workers;
#   - collect results and drop result files into `./scalems_done`.
#

# ------------------------------------------------------------------------------
#
class ScaleMSMaster(rp.task_overlay.Master):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rp.task_overlay.Master.__init__(self, cfg=cfg)

        sbox = os.environ['RP_PILOT_SANDBOX']

        self._dir_new     = '%s/scalems_new'     % sbox
        self._dir_pending = '%s/scalems_pending' % sbox
        self._dir_active  = '%s/scalems_active'  % sbox
        self._dir_done    = '%s/scalems_done'    % sbox

        ru.rec_makedir(self._dir_new)
        ru.rec_makedir(self._dir_pending)
        ru.rec_makedir(self._dir_active)
        ru.rec_makedir(self._dir_done)

        self._results = list()
        self._term    = mt.Event()
        self._thread  = mt.Thread(target=self._ingest)
        self._thread.daemon = True
        self._thread.start()


    # --------------------------------------------------------------------------
    #
    def _ingest(self):

        try:

            new  = self._dir_new
            pend = self._dir_pending
            act  = self._dir_active

            while not self._term.is_set():

                incoming = list()
                self._log.debug('=== check %s' % new)

                for fname in glob.glob('%s/*.json' % new):

                    self._log.debug('=== found %s' % fname)

                    # find incoming work requests
                    # FIXME: ensure that write is complete
                    base = os.path.basename(fname)
                    ru.sh_callout('mv %s/%s %s/%s' % (new, base, pend, base))
                    incoming.append(base)

                for base in incoming:

                    self._log.debug('=== work  %s' % base)

                    # read work description and submit as task
                    request = ru.read_json('%s/%s' % (pend, base))
                    request['data']['base'] = base
                    self.request(request)

                    # that work is now active
                    ru.sh_callout('mv %s/%s %s/%s' % (pend, base, act, base))

                if not incoming:
                    # avoid busy loop
                    time.sleep(1)

        except:
            self._log.exception('ingest thread failed')
            raise


    # --------------------------------------------------------------------------
    #
    def create_work_items(self):

        # nothing to do here
        self._log.debug('=== create_work_items()')
        pass


    # --------------------------------------------------------------------------
    #
    def result_cb(self, requests):

        act  = self._dir_active
        done = self._dir_done

        for r in requests:

            print('result_cb %s: %s [%s]' % (r.uid, r.state, r.result))

            # that work is now don
            base = r.work['data']['base']
            ru.sh_callout('mv %s/%s %s/' % (act, base, done))

        # FIXME: we actually finish after first result is received
        self.stop()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    cfg    = ru.Config(cfg=ru.read_json(sys.argv[1]))
    master = ScaleMSMaster(cfg)

    master.submit(descr=cfg.worker_descr, count=cfg.n_workers,
                  cores=cfg.cpn, gpus=cfg.gpn)

    master.start()

    print('master started')

    while master.alive():
        print('master alive')
        time.sleep(5)

    print('master stopped')

    # FIXME: clean up workers on termination


# ------------------------------------------------------------------------------

