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
class ScaleMSMaster(rp.raptor.Master):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rp.raptor.Master.__init__(self, cfg=cfg)

        self._log = ru.Logger(self.uid, ns='radical.pilot')

        self._dir_new     = './new'
        self._dir_pending = './pending'
        self._dir_active  = './active'
        self._dir_done    = './done'

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
            done = self._dir_done
            stop = False

            while not self._term.is_set():

                incoming = list()
                self._log.debug('=== check %s' % new)

                for fname in glob.glob('%s/*.json' % new):

                    self._log.debug('=== found %s' % fname)

                    work = ru.read_json(fname)
                    if work.get('cmd') == 'stop':
                        print('stopping')
                        stop = True
                        ru.sh_callout('mv %s %s' % (fname, done))
                        continue

                    if stop:
                        self._log.debug('=== ignore %s' % fname)
                        ru.sh_callout('mv %s %s' % (fname, done))
                        continue

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
            self.stop()
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
            ru.write_json(r.as_dict(), '%s/%s.json' % (done, base))
            ru.sh_callout('rm %s/%s' % (act,  base))

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
    master.join()
    time.sleep(100)
    master.stop()


# ------------------------------------------------------------------------------

