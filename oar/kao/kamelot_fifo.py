#!/usr/bin/env python
# coding: utf-8

import copy

from procset import ProcSet

from sqlalchemy.orm import scoped_session, sessionmaker
from oar.kao.platform import Platform
from oar.kao.scheduling_basic import find_resource_hierarchies_job
from oar.lib.globals import get_logger, init_oar

config, db, log = init_oar(no_db=True)


logger = get_logger("oar.kamelot_fifo")


def schedule_fifo_cycle(session, config, plt, queue="default", hierarchy_use=False):
    assigned_jobs = {}

    now = plt.get_time()

    logger.info("Begin scheduling....now: " + str(now) + ", queue: " + queue)

    #
    # Retrieve waiting jobs
    #
    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(
        queue, session=session
    )

    if nb_waiting_jobs > 0:
        logger.info("nb_waiting_jobs:" + str(nb_waiting_jobs))
        for jid in waiting_jids:
            logger.debug("waiting_jid: " + str(jid))

        #
        # Determine Global Resource Intervals
        #
        resource_set = plt.resource_set(session, config)
        res_itvs = copy.copy(resource_set.roid_itvs)

        #
        # Get  additional waiting jobs' data
        #
        job_security_time = int(config["SCHEDULER_JOB_SECURITY_TIME"])
        plt.get_data_jobs(
            session, waiting_jobs, waiting_jids, resource_set, job_security_time
        )

        #
        # Remove resources used by running job
        #
        for job in plt.get_scheduled_jobs(
            session, resource_set, job_security_time, now
        ):
            if job.state == "Running":
                res_itvs = res_itvs - job.res_itvs

        #
        # Assign resource to jobs
        #

        for jid in waiting_jids:
            job = waiting_jobs[jid]

            # We consider only one instance of resources request (no support for moldable)
            (mld_id, _, hy_res_rqts) = job.mld_res_rqts[0]

            if hierarchy_use:
                # Assign resources which hierarchy support (uncomment)
                itvs = find_resource_hierarchies_job(
                    res_itvs, hy_res_rqts, resource_set.hierarchy
                )
            else:
                # OR assign resource by considering only resource_id (no hierarchy)
                # and only one type of resource
                (hy_level_nbs, constraints) = hy_res_rqts[0]
                (_, nb_asked_res) = hy_level_nbs[0]
                itvs_avail = constraints & res_itvs
                ids_avail = list(itvs_avail)

                if len(ids_avail) < nb_asked_res:
                    itvs = ProcSet()
                else:
                    itvs = ProcSet(*ids_avail[:nb_asked_res])

            if len(itvs) != 0:
                job.moldable_id = mld_id
                job.res_set = itvs
                assigned_jobs[job.id] = job
                res_itvs = res_itvs - itvs
            else:
                logger.debug(
                    "Not enough available resources, it's a FIFO scheduler, we stop here."
                )
                break

        #
        # Save assignement
        #
        logger.info("save assignement")
        plt.save_assigns(session, assigned_jobs, resource_set)

    else:
        logger.info("no waiting jobs")


#
# Main function
#
def main(session=None, config=None):
    if not session:
        config, engine, log = init_oar(config)

        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        session = scoped()
    
    logger = get_logger("oar.kamelot_basic", forward_stderr=True)

    config["LOG_FILE"] = "/tmp/oar_kamelot.log"
    logger = get_logger("oar.kamelot_fifo", forward_stderr=True)
    plt = Platform()
    schedule_fifo_cycle(session, config, plt, "default")
    logger.info("That's all folks")


if __name__ == "__main__":  # pragma: no cover
    main()
