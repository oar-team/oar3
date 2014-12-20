from oar import config, get_logger, Job
from platform import Platform
from slot import SlotSet, Slot
from scheduling import schedule_id_jobs_ct

# Initialize some variables to default value or retrieve from oar.conf configuration file *)


#config['LOG_FILE'] = '/dev/stdout'
#log = get_logger("oar.kamelot")

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
                  "SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER": "30",
}
for k,v in default_config.iteritems():
    if not k in config:
        config[k] = k

#               #
# Suspend stuff #
#               #
# TODO

def schedule_cycle(plt, queue = "default"):
    now = plt.get_time()

    print "Begin scheduling....", now

    #
    # Retrieve waiting jobs
    #

    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(queue)

    print waiting_jobs, waiting_jids, nb_waiting_jobs

    if nb_waiting_jobs > 0:

        #
        # Determine Global Resource Intervals and Initial Slot
        #
        resource_set = plt.resource_set()
        initial_slot_set = SlotSet(Slot(1, 0, 0, resource_set.roid_itvs, now, max_time))

        #
        #  Resource availabilty (Available_upto field) is integrated through pseudo job
        #
        pseudo_jobs = []
        for t_avail_upto in sorted(resource_set.available_upto.keys()):
            itvs = resource_set.available_upto[t_avail_upto]
            j = Job()
            print t_avail_upto, max_time - t_avail_upto, itvs
            j.start_time = t_avail_upto
            j.walltime = max_time - t_avail_upto
            j.res_set = itvs

            pseudo_jobs.append(j)

        if pseudo_jobs != []:
            initial_slot_set.split_slots_prev_scheduled_jobs(pseudo_jobs)

        #
        # Get  additional waiting jobs' data
        #
        plt.get_data_jobs(waiting_jobs, waiting_jids, resource_set)

        #
        # Karma sorting (Fairsharing) 
        #
        if config["FAIRSHARING_ENABLED"] == "yes":
            karma_jobs_sorting(queue, now, jids, jobs, plt)

        #
        # Get already scheduled jobs advanced reservations and jobs from more higher priority queues
        #
        scheduled_jobs = plt.get_scheduled_jobs(resource_set)

        if scheduled_jobs != []:
            initial_slot_set.split_slots_prev_scheduled_jobs(scheduled_jobs)

        #print "after split sched"
        initial_slot_set.show_slots()

        all_slot_sets = {0:initial_slot_set}

        #
        # Scheduled
        #
        schedule_id_jobs_ct(all_slot_sets, waiting_jobs , resource_set.hierarchy,  waiting_jids, 20)

        #
        # Save assignement
        #

        plt.save_assigns(waiting_jobs, resource_set)
    else:
        print "no waiting jobs"

#
# Main function
#

if __name__ == '__main__':
    plt = Platform()
    schedule_cycle(plt)
    print "That's all folks"

