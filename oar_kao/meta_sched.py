import os
import sys
import subprocess
from oar.lib import config, Queue
from oar.kao.job import get_current_not_waiting_jobs, get_gantt_jobs_to_launch

# Log category
log = get_logger("oar.kao")

exit_code = 0

#order_part = config["SCHEDULER_RESOURCE_ORDER"]

###########################################################################################################
#Initialize Gantt tables with scheduled reservation jobs, Running jobs, toLaunch jobs and Launching jobs; #
###########################################################################################################

# First get resources that we must add for each reservation jobs
#TODO

# Take care of the currently (or nearly) running jobs
# Lock to prevent bipbip update in same time
#OAR::IO::lock_table($base,["jobs","assigned_resources","gantt_jobs_predictions","gantt_jobs_resources","job_types","moldable_job_descriptions","resources","job_state_logs","gantt_jobs_predictions_log","gantt_jobs_resources_log"]);

#calculate now date with no overlap with other jobs

#Init the gantt chart with all resources

# Add already scheduled reservations into the gantt



#oar_debug("[MetaSched] End processing of waiting reservations\n");

###########################################################################################################
# End init scheduler                                                                                      #
###########################################################################################################

# launch right reservation jobs
# arg : queue name
# return 1 if there is at least a job to treate, 2 if besteffort jobs must die


###########



###########



# Prepare a job to be run by bipbip
def prepare_jobs_to_be_launched():
  #      $job_id,
  #      $moldable_job_id,
  #      $job_submission_time,
  #      $resources_array_ref) = @_;

    #my $running_date = $current_time_sec;
    #if ($running_date < $job_submission_time){
    #    $running_date = $job_submission_time;
    #}
    
    tuple_jids = tuple(jids) 
    #set start_time for jobs to launch 
    set_jobs_start_time(tuple_jids, start_time)

    # OAR::IO::set_assigned_moldable_job($base, $job_id, $moldable_job_id);
    for job in jobs:
        set_assigned_moldable_job(job)

        
    #OAR::IO::add_resource_job_pairs($base, $moldable_job_id, $resources_array_ref);
    #TODO
    add_resource_jobs(tuple_modable_ids)

    #OAR::IO::set_job_state($base, $job_id, "toLaunch");
    # ser

    #set_jobs_state(tuple_jids, "toLaunch")
    
    #notify_to_run_jobs


def treate_waiting_reservation_jobs(name):
    pass

def check_reservation_jobs(name):
    pass

def check_jobs_to_kill():
    log.debug("Begin precessing of besteffort jobs to kill")
    #my %nodes_for_jobs_to_launch;
    #if (defined $redis) {
    #    %nodes_for_jobs_to_launch = OAR::IO::get_gantt_resources_for_jobs_to_launch_redis($base,$redis,$current_time_sec);
    #}else{
    #    %nodes_for_jobs_to_launch = OAR::IO::get_gantt_resources_for_jobs_to_launch($base,$current_time_sec);
    #}
    return 0

def check_jobs_to_launch(current_time_sec, current_time_sql):
    log.debug("Begin processing of jobs to launch (start time <= " + current_time_sql)

    return_code = 0
    #TODO
    #job to launch
    jobs_to_launch_moldable_id_req = get_gantt_jobs_to_launch(current_time_sec)

    #my $return_code = 0;
    #my %jobs_to_launch = (); 
    #    %jobs_to_launch = OAR::IO::get_gantt_jobs_to_launch($base,$current_time_sec);
    #} 

    for job_moldable_id in jobs_to_launch_moldable_id_req:
        return_code = 1
        log.debug("Set job " + str(job.id) + " state to toLaunch at " + current_time_sql)
        job, moldable_id = job_moldable_id

        #TODO
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

    if return_code == 1:
        prepare_jobs_to_be_launched()
    #    prepare_job_to_be_launched($base, $i, $jobs_to_launch{$i}->[0], $job->{submission_time}, $jobs_to_launch{$i}->[1]);

    log.debug("End processing of jobs to launch");

    return 0


def update_gantt_visualization():
    sql_queries = ["TRUNCATE TABLE gantt_jobs_predictions_visu",
                   "TRUNCATE TABLE gantt_jobs_resources_visu",
                   "INSERT INTO gantt_jobs_predictions_visu SELECT * FROM gantt_jobs_predictions",
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
    initial_time_sql = TODO
    #my %initial_time = (
    #                "sec" => $current_time_sec,
    #                "sql" => $current_time_sql
    #              );
    
    if "OARDIR" in os.environ:
        binpath = os.environ["OARDIR"] + "/"
    else:
        log.error("OARDIR env variable must be defined")
        exit

    for queue in Queue.query.order_by("priority ASC").all():
        
        if queue.state == "Active":
            log.debug("Queue " + queue.name + ": Launching scheduler $policy at time " 
                      + initial_time_sql)
          
            
            cmd_schedudler = binpath + "scheduler/" + queue.scheduler_policy 
            
            child_launched = True

            try:
                child = subprocessPopen([cmd_schedudler, queue.name, initial_time_sec,initial_time_sql]
                                        ,stdout=subprocess.PIPE)
                                    
                for line in iter(child.stdout.readline,''):
                    loq.debug( "Read on the scheduler output:" + line.rstrip() )
                #TODO SCHEDULER_LAUNCHER_OPTIMIZATION
                #if ((get_conf_with_default_param("SCHEDULER_LAUNCHER_OPTIMIZATION", "yes") eq "yes") and


                child.wait()
                rc = child.returncode
                sched_exit_code, sched_signal_num, sched_dumped_core = rc >> 8, rc & 0x7f, bool(rc & 0x80)

            except OSError as e:
                child_launched = False
                log.warn(e + " Cannot run: " + cmd_schedudler + " " + queue.name  + " " + 
                         initial_time_sec + " " + initial_time_sql)


            if (not child_launched) or (sched_signal_num != 0) or (sched_dumped_core !=0 ): 
                log.error("Execution of " + queue.scheduler_policy + 
                          " failed, inactivating queue " + queue.name + " (see `oarnotify')")
                #stop queue
                db.query(Queue).filter_by(name=queue.name).update({"state": u"notActive"})


            if sched_exit_code != 1:
                log.error("Scheduler $policy returned a bad value: " + str(sched_exit_code) + 
                          ". Inactivating queue " +  queue.scheduler_policy + " (see `oarnotify')")
                #stop queue
                db.query(Queue).filter_by(name=queue.name).update({"state": u"notActive"})
        
            treate_waiting_reservation_jobs(name)
            check_reservation_jobs(name)

    if check_jobs_to_kill() == 1:
        # We must kill besteffort jobs
        socket_notification.send("ChState")
        exit_code = 2
    elif check_jobs_to_launch() == 1:
        exit_code = 0

    #Update visu gantt tables
    update_gantt_visualization()

    # Manage dynamic node feature
    
    # Send CHECK signal to Hulot if needed

    jobids_by_state, current_not_waiting_jobs = get_current_not_waiting_jobs()


    # Search jobs to resume
    
    # Notify oarsub -I when they will be launched
    
    # Run the decisions
    ## Treate "toError" jobs

    ## Treate toAckReservation jobs

    ## Treate toLaunch jobs
    #foreach my $j (OAR::IO::get_jobs_in_state($base,"toLaunch")){
    #    notify_to_run_job($base, $j->{job_id});
    #}

    log.debug("End of Meta Scheduler")

    return exit_code
