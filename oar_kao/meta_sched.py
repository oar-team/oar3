
# oar/sources/core/modules/scheduler/oar_meta_sched
import time
import os
import sys
import subprocess
from oar.lib import (config, db, Queue, get_logger, 
                     GanttJobsPredictionsVisu, GanttJobsResourcesVisu,
                     GanttJobsPrediction, GanttJobsResource)
from oar.kao.job import (get_current_not_waiting_jobs, get_gantt_jobs_to_launch,
                         set_job_start_time_assigned_moldable_id, add_resource_job_pairs,
                         set_job_state, get_gantt_waiting_interactive_prediction_date,
                         frag_job, get_waiting_reservation_jobs_specific_queue,
                         get_waiting_toSchedule_reservation_jobs_specific_queue)
from oar.kao.utils import (create_almighty_socket, notify_almighty, notify_tcp_socket, 
                           local_to_sql, add_new_event)
from oar.kao.platform import Platform
from oar.kao.job import NO_PLACEHOLDER, JobPseudo
from oar.kao.slot import SlotSet, Slot
from oar.kao.scheduling import set_slots_with_prev_scheduled_jobs

max_time = 2147483648 #(* 2**31 *)
max_time_minus_one = 2147483647 #(* 2**31-1 *)

config['LOG_FILE'] = '/dev/stdout'
# Log category
log = get_logger("oar.kao.meta_sched")

exit_code = 0

# stock the job ids that where already send to almighty
to_launch_jobs_already_treated = {}

#order_part = config["SCHEDULER_RESOURCE_ORDER"]

###########################################################################################################
#Initialize Gantt tables with scheduled reservation jobs, Running jobs, toLaunch jobs and Launching jobs; #
###########################################################################################################


def slots_init(initial_time_sec):

    job_security_time = config["SCHEDULER_JOB_SECURITY_TIME"]
    plt =Platform()
    #
    # Determine Global Resource Intervals and Initial Slot
    #
    resource_set = plt.resource_set()
    initial_slot_set = SlotSet(Slot(1, 0, 0, resource_set.roid_itvs, initial_time_sec, max_time))

    #
    #  Resource availabilty (Available_upto field) is integrated through pseudo job
    #
    pseudo_jobs = []
    for t_avail_upto in sorted(resource_set.available_upto.keys()):
        itvs = resource_set.available_upto[t_avail_upto]
        j = JobPseudo()
        #print t_avail_upto, max_time - t_avail_upto, itvs
        j.start_time = t_avail_upto
        j.walltime = max_time - t_avail_upto
        j.res_set = itvs
        j.ts = False
        j.ph = NO_PLACEHOLDER
        
        pseudo_jobs.append(j)
        
    if pseudo_jobs != []:
        initial_slot_set.split_slots_prev_scheduled_jobs(pseudo_jobs)


    #
    # Get already scheduled jobs advanced reservations and jobs from more higher priority queues
    #
    scheduled_jobs = plt.get_scheduled_jobs(resource_set, job_security_time, initial_time_sec)
    all_slot_sets = {0:initial_slot_set}
    if scheduled_jobs != []:
        filter_besteffort = True
        set_slots_with_prev_scheduled_jobs(all_slot_sets, scheduled_jobs,
                                           job_security_time, filter_besteffort) 

    #get ressources
    #get running jobs exclude BE jobs
    #get sheduled AR jobs
    #remove resources if there are missing for scheduled AR jobs,
    #fill slots
    #delete GanttJobsprediction and GanttJobsResource
    #set  GanttJobsprediction and GanttJobsResource
    

    return all_slot_sets


# First get resources that we must add for each reservation jobs
#oar_debug("[MetaSched] Resources to automatically add to all reservations: @Resources_to_always_add\n");
#TODO

# Take care of the currently (or nearly) running jobs
# Lock to prevent bipbip update in same time
#OAR::IO::lock_table($base,["jobs","assigned_resources","gantt_jobs_predictions","gantt_jobs_resources","job_types","moldable_job_descriptions","resources","job_state_logs","gantt_jobs_predictions_log","gantt_jobs_resources_log"]);

#calculate now date with no overlap with other jobs


#oar_debug("[MetaSched] Retrieve information for already scheduler reservations from database before flush (keep assign resources)\n");

#Init the gantt chart with all resources

#oar_debug("[MetaSched] Begin processing of already handled reservations\n");
# Add already scheduled reservations into the gantt
# A container job cannot be placeholder or allowed or timesharing. 
#Fill all other gantts

#oar_debug("[MetaSched] End processing of waiting reservations\n");

#oar_debug("[MetaSched] Begin processing of current jobs\n");
#
#
#oar_debug("[MetaSched] End processing of current jobs\n");

#oar_debug("[MetaSched] Begin processing of accepted reservations which do not have assigned resources yet\n");

#oar_debug("[MetaSched] End processing of accepted reservations which do not have assigned resources yet\n");

###########################################################################################################
# End init scheduler                                                                                      #
###########################################################################################################

# launch right reservation jobs
# arg : queue name
# return 1 if there is at least a job to treate, 2 if besteffort jobs must die

#sub treate_waiting_reservation_jobs($){
#oar_debug("[MetaSched] Queue $queue_name: begin processing of reservations with missing resources\n");
#oar_debug("[MetaSched] Queue $queue_name: end processing of reservations with missing resources\n");


###########



###########

# Tell Almighty to run a job
# sub notify_to_run_job($$){
def notify_to_run_job(jid):

    if not jid in to_launch_jobs_already_treated:
        if 0: #TODO OAR::IO::is_job_desktop_computing
            log.debug(str(jid) + ": Desktop computing job, I don't handle it!")
        else:
            nb_sent = notify_almighty("OARRUNJOB_" + str(jid))
            if nb_sent:
                to_launch_jobs_already_treated[jid] = 1
                log.debug("Notify almighty to launch the job" + str(jid))
            else:
                log.warn("Not able to notify almighty to launch the job " + str(jid) + " (socket error)")

# Prepare a job to be run by bipbip
def prepare_job_to_be_launched(job, moldable_id, current_time_sec):
    jid = job.id
    
    #TODO ??? 
    #my $running_date = $current_time_sec;
    #if ($running_date < $job_submission_time){
    #    $running_date = $job_submission_time;
    #}

    #OAR::IO::set_running_date_arbitrary($base, $job_id, $running_date);
    #OAR::IO::set_assigned_moldable_job($base, $job_id, $moldable_job_id);

    #set start_time an for jobs to launch 
    set_job_start_time_assigned_moldable_id(jid, current_time_sec, moldable_id)
        
    #OAR::IO::add_resource_job_pairs($base, $moldable_job_id, $resources_array_ref);
    add_resource_job_pairs(moldable_id)
    #OAR::IO::set_job_state($base, $job_id, "toLaunch");
    set_job_state(jid, "toLaunch")
    
    notify_to_run_job(jid)

# launch right reservation jobs
# arg : queue name
# return 1 if there is at least a job to treate, 2 if besteffort jobs must die
def treate_waiting_reservation_jobs(queue_name):
    log.debug("Queue " + queue_name + ": begin processing of reservations with missing resources")
    for job in get_waiting_reservation_jobs_specific_queue(queue_name):
        
        if (current_time_sec > (job.start_time + walltime)):
            log.warn("[" + str(job.id) + "] set job state to Error: reservation expired and couldn't be started")
            set_job_state(job.id, "Error")
            set_job_message(job.id, "Reservation expired and couldn't be started.")
            
    #TOD0
    #log.warn("[" + job.id + "] reservation is waiting because no resource is present")

    #TOFINISH

def check_reservation_jobs(queue_name):
    log.debug("Queue " + queue_name + ": begin processing of new reservations")
            
    # Find jobs to check
    ar_jobs = get_waiting_toSchedule_reservation_jobs_specific_queue(queue_name)
    if ar_jobs != []:
        log.debug("Find resource for Advance Reservation job:" + str(job.id))

        #TODO: test if container is an AR job
        ss_id = 0
        if "inner" in job.types:
            ss_id = int(job.types)


def check_jobs_to_kill():

    # Detect if there are besteffort jobs to kill
    # return 1 if there is at least 1 job to frag otherwise 0

    log.debug("Begin precessing of besteffort jobs to kill")
    #my %nodes_for_jobs_to_launch;
    #if (defined $redis) {
    #    %nodes_for_jobs_to_launch = OAR::IO::get_gantt_resources_for_jobs_to_launch_redis($base,$redis,$current_time_sec);
    #}else{
    #    %nodes_for_jobs_to_launch = OAR::IO::get_gantt_resources_for_jobs_to_launch($base,$current_time_sec);
    #}

    log.debug("End precessing of besteffort jobs to kill\n")
    return 0

def check_jobs_to_launch(current_time_sec, current_time_sql):
    log.debug("Begin processing of jobs to launch (start time <= " + current_time_sql)
    
    return_code = 0
    #TODO
    #job to launch
    jobs_to_launch_moldable_id_req = get_gantt_jobs_to_launch(current_time_sec)

    for job_moldable_id in jobs_to_launch_moldable_id_req:
        return_code = 1
        job, moldable_id = job_moldable_id
        log.debug("Set job " + str(job.id) + " state to toLaunch at " + current_time_sql)

        #
        # Advance Reservation
        #
        #TODO start_time ???
        if ((job.reservation == "Scheduled") and (job.start_time < current_time_sec)):
            max_time = jobs_ar[job.id].walltime - (current_time_sec - job.start_time)
            #TOFINISH
        #if (($job->{reservation} eq "Scheduled") and ($job->{start_time} < $current_time_sec)){
        #   my $max_time = $mold->{moldable_walltime} - ($current_time_sec - $job->{start_time});
        #   OAR::IO::set_moldable_job_max_time($base,$jobs_to_launch{$i}->[0], $max_time);
        #   OAR::IO::set_gantt_job_startTime($base,$jobs_to_launch{$i}->[0],$current_time_sec);
        #   oar_warn("[MetaSched] Reduce walltime of job $i to $max_time (was  $mold->{moldable_walltime})\n");
        #   OAR::IO::add_new_event($base,"REDUCE_RESERVATION_WALLTIME",$i,"Change walltime from $mold->{moldable_walltime} to $max_time");
        #        my $w = OAR::IO::duration_to_sql($max_time);                                                                            
        #        if ($job->{message} =~ s/W\=\d+\:\d+\:\d+/W\=$w/g){                                                               
        #            OAR::IO::set_job_message($base,$i,$job->{message});                                                                  
        #        }
        #    }
        
        prepare_job_to_be_launched(job, moldable_id, current_time_sec)

    log.debug("End processing of jobs to launch");

    return return_code


def update_gantt_visualization():

    db.query(GanttJobsPredictionsVisu).delete()
    db.query(GanttJobsResourcesVisu).delete()
    db.commit()

    sql_queries = ["INSERT INTO gantt_jobs_predictions_visu SELECT * FROM gantt_jobs_predictions",
                   "INSERT INTO gantt_jobs_resources_visu SELECT * FROM gantt_jobs_resources"
                   ]
    for query in sql_queries:
        result = db.engine.execute(query)

def meta_schedule():

    exit_code = 0

    create_almighty_socket()

    #delete previous prediction
    #TODO: to move ?

    db.query(GanttJobsPrediction).delete()
    db.query(GanttJobsResource).delete()
    db.commit()

    # reservation ??.
    
    initial_time_sec = time.time()
    initial_time_sql = local_to_sql(initial_time_sec)

    current_time_sec = initial_time_sec
    current_time_sql = initial_time_sql

    all_slot_sets = slots_init(initial_time_sec)
    

    
    if "OARDIR" in os.environ:
        binpath = os.environ["OARDIR"] + "/"
    else:
        binpath = "/usr/local/lib/oar/"
        log.warning("OARDIR env variable must be defined, " + binpath + " is used by default")

    for queue in db.query(Queue).order_by("priority DESC").all():
        
        if queue.state == "Active":
            log.debug("Queue " + queue.name + ": Launching scheduler " + queue.scheduler_policy + " at time " 
                      + initial_time_sql)
          
            cmd_scheduler = binpath + "schedulers/" + queue.scheduler_policy 
            
            child_launched = True
            #TODO TO CONFIRM
            sched_exit_code = 0
            sched_signal_num = 0 
            sched_dumped_core = 0
            try:
                child = subprocess.Popen([cmd_scheduler, queue.name, str(initial_time_sec), initial_time_sql]
                                        ,stdout=subprocess.PIPE)
                                    
                for line in iter(child.stdout.readline,''):
                    log.debug( "Read on the scheduler output:" + line.rstrip() )
                #TODO SCHEDULER_LAUNCHER_OPTIMIZATION
                #if ((get_conf_with_default_param("SCHEDULER_LAUNCHER_OPTIMIZATION", "yes") eq "yes") and

                child.wait()
                rc = child.returncode
                sched_exit_code, sched_signal_num, sched_dumped_core = rc >> 8, rc & 0x7f, bool(rc & 0x80)

            except OSError as e:
                child_launched = False
                log.warn(str(e) + " Cannot run: " + cmd_scheduler + " " + queue.name  + " " + 
                         str(initial_time_sec) + " " + initial_time_sql)


            if (not child_launched) or (sched_signal_num != 0) or (sched_dumped_core !=0 ): 
                log.error("Execution of " + queue.scheduler_policy + 
                          " failed, inactivating queue " + queue.name + " (see `oarnotify')")
                #stop queue
                db.query(Queue).filter_by(name=queue.name).update({"state": "notActive"})


            if sched_exit_code != 0:
                log.error("Scheduler " + queue.scheduler_policy + " returned a bad value: " + str(sched_exit_code) + 
                          ". Inactivating queue " +  queue.scheduler_policy + " (see `oarnotify')")
                #stop queue
                db.query(Queue).filter_by(name=queue.name).update({"state": "notActive"})
        
            treate_waiting_reservation_jobs(queue.name)
            check_reservation_jobs(queue.name)

    if check_jobs_to_kill() == 1:
        # We must kill besteffort jobs
        notify_almighty("ChState")
        exit_code = 2
    elif check_jobs_to_launch(current_time_sec, current_time_sql) == 1:
        exit_code = 0

    #Update visu gantt tables
    update_gantt_visualization()

    # Manage dynamic node feature    
    # Send CHECK signal to Hulot if needed
    # TODO


    jobs_by_state = get_current_not_waiting_jobs()

    # Search jobs to resume
    # TODO

    # Notify oarsub -I when they will be launched
    for j_info in get_gantt_waiting_interactive_prediction_date():
        job_id, job_info_type, job_start_time, job_message = j_info
        addr, port = job_info_type.split(':')
        new_start_prediction = local_to_sql(job_start_time)
        log.debug("[" + str(job_id) + "] Notifying user of the start prediction: " +\
                  new_start_prediction + "(" + job_message + ")")
        notify_tcp_socket(addr,port,"[" + initial_time_sql + "] Start prediction: " +\
                          new_start_prediction + " (" + job_message + ")")

    # Run the decisions
    ## Treate "toError" jobs
    if "toError" in jobs_by_state:
        for job in jobs_by_state["toError"]:
            addr, port = job.info_type.split(':')
            if job.type == "INTERACTIVE" or\
               (job.type == "PASSIVE" and job.reservation == "Scheduled"):
                log.debug("Notify oarsub job (num:" + str(job.id) + ") in error; jobInfo=" +\
                          job.info_type)
                
                nb_sent1 = notify_tcp_socket(addr, port, job.message)
                nb_sent2 = notify_tcp_socket(addr, port, "BAD JOB")
                if (nb_sent1 == 0) or (nb_sent2 == 0):
                    log.warnt("Cannot open connection to oarsub client for" + str(job.id))
            log.debug("Set job " + str(job.id) + " to state Error")
            set_job_state(job.id, "Error")

    ## Treate toAckReservation jobs
    if "toAckReservation" in jobs_by_state:
        for job in jobs_by_state["toAckReservation"]:
            addr, port = job.info_type.split(':')
            log.debug(" Treate job" + job.id + " in toAckReservation state")

            nb_sent = notify_tcp_socket(addr, port,"GOOD RESERVATION")
        
            if nb_sent == 0:
                log.warn("Frag job " + job.id + ", I cannot notify oarsub for the reservation")
                add_new_event("CANNOT_NOTIFY_OARSUB", job.id, "Can not notify oarsub for the job " + job.id)
                
                #TODO ???
                #OAR::IO::lock_table($base,["frag_jobs","event_logs","jobs"]);
                frag_job(job.id)
                #TODO ???
                #OAR::IO::unlock_table($base)
            
                exit_code = 2
            else:
                log.debug("Notify oarsub for a RESERVATION (idJob=" + job.id + ") --> OK; jobInfo=" + job.info_type)
                set_job_state(job.id, "Waiting")
                if ((j.start_time-1) <= current_time_sec) and (exit_code == 0):
                    exit_code = 1

    ## Treate toLaunch jobs
    if "toLaunch" in jobs_by_state:
        for job in jobs_by_state["toLaunch"]:
            notify_to_run_job(job.id)

    log.debug("End of Meta Scheduler")

    return exit_code
