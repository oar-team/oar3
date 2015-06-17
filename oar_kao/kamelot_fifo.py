#!/usr/bin/env python
from oar.lib import config
from oar.kao.platform import Platform
from oar.kao.interval import sub_intervals 

# Initialize some variables to default value or retrieve from oar.conf
# configuration file *)

# Set undefined config value to default one
default_config = {
    "HIERARCHY_LABEL": "resource_id,network_address",
    "SCHEDULER_RESOURCE_ORDER": "resource_id ASC"
}

config.setdefault_config(default_config)

def schedule_fifo_cycle(plt, queue="default"):
    now = plt.get_time()

    print "Begin scheduling....", now

    #
    # Retrieve waiting jobs
    #

    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(queue)

    print waiting_jobs, waiting_jids, nb_waiting_jobs

    if nb_waiting_jobs > 0:

        #
        # Determine Global Resource Intervals
        #
        resource_set = plt.resource_set()
        res_itvs = resource_set.roid_itvs
        
        #
        # Get  additional waiting jobs' data
        #
        plt.get_data_jobs(waiting_jobs, waiting_jids, resource_set)
        

        #
        # Remove resources used by running job 
        #
        for job in plt.get_scheduled_jobs(resource_set):
            if job.state == "Running":
                res_itvs = sub_intervals(res_itvs, job.res_itvs)

        #        
        # Assign resource to jobs
        #        
        for jid in waiting_jids:
            job = waiting_jobs[jid]
            # We consider only one instance of resources request (no support for moldable)
            (mld_id, walltime, hy_res_rqts) = job.mld_res_rqts[0]
            itvs = find_resource_hierarchies_job(itvs_avail, hy_res_rqts, resource_set.hierarchy)

            if (itvs != []):
                job.res_itvs = itvs
            else:
                print "Not enough available resources, it's a FIFO scheduler, we stop here."
                break
        
        #
        # Save assignement
        #

        plt.save_assigns(waiting_jobs, resource_set)

    else:
        print "No waiting jobs"

#
# Main function
#

if __name__ == '__main__':
    plt = Platform()
    schedule_fifo_cycle(plt)
    print "That's all folks"
