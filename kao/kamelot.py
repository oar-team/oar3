import time
from oar import config
from resource import ResourceSet
from job import Job, get_waiting_jobs, get_data_jobs
from slot import SlotSet, Slot
import scheduling

# Initialize some variables to default value or retrieve from oar.conf configuration file *)

besteffort_duration = 5*60
max_time = 2147483648 #(* 2**31 *)
max_time_minus_one = 2147483647 #(* 2**31-1 *)
# Constant duration time of a besteffort job *)
besteffort_duration = 300

#Set undefined config value to default one
default_config = {"HIERARCHY_LABEL": "resource_id,network_address",
                  "SCHEDULER_RESOURCE_ORDER": "resource_id ASC",
                  "SCHEDULER_JOB_SECURITY_TIME": "60",
                  "FAIRSHARING_ENABLED": "no",
                  "SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER": "30"
}
for k,v in default_config.iteritems():
    if not k in config:
        config[k] = k

#
# for TOKEN feature
# SCHEDULER_TOKEN_SCRIPTS="{ fluent => '/usr/local/bin/check_fluent.sh arg1 arg2', soft2 => '/usr/local/bin/check_soft2.sh arg1' }" *)
# TODO

#
# Karma and Fairsharing stuff *)
#
if config["FAIRSHARING_ENABLED"] == "yes": 
    fairsharing_nb_job_limit = config["SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER"]
    karma_window_size =  3600 * 30 * 24 # TODO in conf ???
    
#TODO
#
# Sort jobs accordingly to karma value (fairsharing)  *)
#
#               #
# Suspend stuff #
#               #


#                
# Main function
#
if True or __name__ == '__main__':
    now = int(time.time())

    #
    # Retreive waiting jobs
    #
    queue = "test"
    waiting_jobs, waiting_jids, nb_waiting_jobs = get_waiting_jobs(queue)

    print waiting_jobs, waiting_jids, nb_waiting_jobs


    if True or nb_waiting_jobs > 1:


        #                                                                                
        # Determine Global Resource Intervals and Initial Slot                           
        #
        resource_set = ResourceSet()
        initial_slot_set = SlotSet(Slot(1, 0, 0, resource_set.roid_itvs, now, max_time))

        #
        #  Resource availabilty (Available_upto field) is integrated through pseudo job
        #
        pseudo_jobs = []
        for t_avail_upto in sorted(resource_set.available_upto.keys()):
            itvs = resource_set.available_upto[t_avail_upto]
            j = Job()
            print t_avail_upto, max_time - t_avail_upto, itvs
            j.pseudo(t_avail_upto, max_time - t_avail_upto, itvs)
            pseudo_jobs.append(j)
        
        initial_slot_set.split_slots_prev_scheduled_jobs(pseudo_jobs)

        #
        # Get  additionalwaiting jobs' data
        #
        get_data_jobs(waiting_jobs, waiting_jids, resource_set)
            
        #
        # get get_scheduled_jobs
        #
        #get_scheduled_jobs()

        all_slot_sets = {0:initial_slot_set}

        #
        # Scheduled
        #
    


print "yopa"
        
