
# oar/sources/core/modules/scheduler/oar_meta_sched
import time
import os
import sys
import subprocess
from oar.lib import config, db, Queue, get_logger, GanttJobsPredictionsVisu, GanttJobsResourcesVisu
from oar.kao.job import (get_current_not_waiting_jobs, get_gantt_jobs_to_launch,
                         set_job_start_time_assigned_moldable_id, add_resource_job_pairs,
                         set_job_state)
from oar.kao.utils import create_tcp_notification_socket, local_to_sql

config['LOG_FILE'] = '/dev/stdout'
# Log category
log = get_logger("oar.kao.meta_sched")

exit_code = 0

#order_part = config["SCHEDULER_RESOURCE_ORDER"]

###########################################################################################################
#Initialize Gantt tables with scheduled reservation jobs, Running jobs, toLaunch jobs and Launching jobs; #
###########################################################################################################

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
    log.debug("notify_to_run_job not implemented !!!!!!!!!!!")
    pass

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


# advance reservation job to launch ?
def treate_waiting_reservation_jobs(name):
    pass

def check_reservation_jobs(name):
    #oar_debug("[MetaSched] Queue $queue_name: begin processing of new reservations\n");
    #big 
    #oar_debug("[MetaSched] Queue $queue_name: end processing of new reservations\n");
    pass

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

        #TODO Advance Reservation
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
    #GanttJobsPredictionsVisu.query.delete()
    #GanttJobsResourcesVisu.query.delete()
    db.commit()
    sql_queries = ["INSERT INTO gantt_jobs_predictions_visu SELECT * FROM gantt_jobs_predictions",
                   "INSERT INTO gantt_jobs_resources_visu SELECT * FROM gantt_jobs_resources"
                   ]
    for query in sql_queries:
        result = db.engine.execute(query)

def get_current_jobs_not_waiting():
    pass

def meta_schedule():

    exit_code = 0

    create_tcp_notification_socket()

    # reservation ??.
    
    initial_time_sec = time.time()
    initial_time_sql = local_to_sql(initial_time_sec)

    current_time_sec = initial_time_sec
    current_time_sql = initial_time_sql

    #my %initial_time = (
    #                "sec" => $current_time_sec,
    #                "sql" => $current_time_sql
    #              );
    
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
        socket_notification.send("ChState")
        exit_code = 2
    elif check_jobs_to_launch(current_time_sec, current_time_sql) == 1:
        exit_code = 0

    #Update visu gantt tables
    update_gantt_visualization()

    # Manage dynamic node feature    
    # Send CHECK signal to Hulot if needed

    jobs_by_state = get_current_not_waiting_jobs()

    # Search jobs to resume
    # Notify oarsub -I when they will be launched
    # Run the decisions
    ## Treate "toError" jobs
    ## Treate toAckReservation jobs

    ## Treate toLaunch jobs
    if "toLaunch" in jobs_by_state:
        for job in jobs_by_state["toLaunch"]:
            notify_to_run_job(job.id)

    log.debug("End of Meta Scheduler")

    return exit_code
