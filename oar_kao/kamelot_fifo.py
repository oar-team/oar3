#!/usr/bin/env python

from oar.lib import config, get_logger
from oar.kao.platform import Platform
from oar.kao.interval import (intersec, sub_intervals, itvs2ids, unordered_ids2itvs)
from oar.kao.scheduling_basic import find_resource_hierarchies_job
# Initialize some variables to default value or retrieve from oar.conf
# configuration file *)

# Set undefined config value to default one
DEFAULT_CONFIG = {
    'DB_PORT': '5432',
    'HIERARCHY_LABEL': 'resource_id,network_address',
    'SCHEDULER_RESOURCE_ORDER': "resource_id ASC",
    'SCHEDULER_JOB_SECURITY_TIME': '60',
    'SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE': 'default',
    'FAIRSHARING_ENABLED': 'no',
}

config.setdefault_config(DEFAULT_CONFIG)

log = get_logger("oar.kamelot_fifo")
#config['LOG_FILE'] = '/tmp/oar_kamelot.log'

def schedule_fifo_cycle(plt, queue="default", hierarchy_use = False):

    assigned_jobs = {}

    now = plt.get_time()

    log.info("Begin scheduling....now: " + str(now) + ", queue: " + queue)

    #
    # Retrieve waiting jobs
    #

    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(queue)


    if nb_waiting_jobs > 0:
        log.info("nb_waiting_jobs:" + str(nb_waiting_jobs))
        for jid in waiting_jids:
             log.debug("waiting_jid: " + str(jid))

        #
        # Determine Global Resource Intervals
        #
        resource_set = plt.resource_set()
        res_itvs = resource_set.roid_itvs

        #
        # Get  additional waiting jobs' data
        #
        job_security_time = int(config["SCHEDULER_JOB_SECURITY_TIME"])
        plt.get_data_jobs(waiting_jobs, waiting_jids, resource_set, job_security_time)

        #
        # Remove resources used by running job
        #
        for job in plt.get_scheduled_jobs(resource_set, job_security_time, now):
            if job.state == "Running":
                res_itvs = sub_intervals(res_itvs, job.res_itvs)

        #
        # Assign resource to jobs
        #
        for jid in waiting_jids:
            job = waiting_jobs[jid]
            # We consider only one instance of resources request (no support for moldable)
            (mld_id, walltime, hy_res_rqts) = job.mld_res_rqts[0]
            if hierarchy_use:
                # Assign resources which hierarchy support (uncomment)
                itvs = find_resource_hierarchies_job(res_itvs, hy_res_rqts, resource_set.hierarchy)
            else:
                # OR assign resource by considering only resource_id (no hierarchy)
                # and only one type of resource
                (hy_level_nbs, constraints) = hy_res_rqts[0]
                (h_name, nb_asked_res) = hy_level_nbs[0]
                itvs_avail = intersec(constraints, res_itvs)
                ids_avail = itvs2ids(itvs_avail)

                if len(ids_avail) < nb_asked_res:
                    itvs = []
                else:
                    itvs = unordered_ids2itvs(ids_avail[:nb_asked_res])

            if (itvs != []):
                job.moldable_id = mld_id
                job.res_set = itvs
                assigned_jobs[job.id] = job
                res_itvs = sub_intervals(res_itvs, itvs)
            else:
                log.debug("Not enough available resources, it's a FIFO scheduler, we stop here.")
                break

        #
        # Save assignement
        #

        log.info("save assignement")
        plt.save_assigns(assigned_jobs, resource_set)

    else:
        log.info("no waiting jobs")

#
# Main function
#

if __name__ == '__main__':
    plt = Platform()
    schedule_fifo_cycle(plt, "default")
    log.info("That's all folks")
