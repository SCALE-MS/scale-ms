#!/usr/bin/env python3

import os
import sys
import glob

import radical.utils as ru
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    cfg_file  = sys.argv[1]
    cfg_dir   = os.path.abspath(os.path.dirname(cfg_file))
    cfg_fname =                 os.path.basename(cfg_file)

    cfg       = ru.Config(cfg=ru.read_json(cfg_file))
    cpn       = cfg.cpn
    gpn       = cfg.gpn
    n_masters = cfg.n_masters
    n_workers = cfg.n_workers
    workload  = cfg.workload

    # each master uses a node, and each worker on each master uses a node
    nodes     =  n_masters + (n_masters * n_workers)
    print('nodes', nodes)

    master    = '%s/%s' % (cfg_dir, cfg.master)
    worker    = '%s/%s' % (cfg_dir, cfg.worker)

    session   = rp.Session()
    try:
        pd = rp.ComputePilotDescription(cfg.pilot_descr)
        pd.cores    = nodes * cpn
        pd.gpus     = nodes * gpn
        pd.runtime  = cfg.pilot_descr.runtime
        pd.resource = cfg.pilot_descr.resource

        tds = list()

        for i in range(n_masters):
            td = rp.ComputeUnitDescription(cfg.master_descr)
            td.executable     = "python3"
            td.cpu_threads    = cpn
            td.gpu_processes  = gpn
            td.arguments      = [master, cfg_file, i]
            td.input_staging  = [{'source': cfg.master,
                                  'target': 'unit://',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': cfg.worker,
                                  'target': 'unit://',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': cfg_file,
                                  'target': 'unit://',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS}
                                ]
            tds.append(td)

        pmgr  = rp.PilotManager(session=session)
        umgr  = rp.UnitManager(session=session)
        pilot = pmgr.submit_pilots(pd)
        tasks = umgr.submit_units(tds)

        umgr.add_pilots(pilot)

        # send work item
        for fname in glob.glob('scalems_work/*.json'):
            check = ru.read_json(fname)
            pilot.stage_in({'source': fname,
                            'target': 'pilot://scalems_new/',
                            'action': rp.TRANSFER,
                            'flags' : rp.DEFAULT_FLAGS})

      # for task in tasks:
      #     task.cancel()

        umgr.wait_units()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

