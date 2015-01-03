import os
import subprocess
from oar import config, Queue

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


def meta_schedule():
    
    # reservation ??.
    
    initial_time_sec = time.time()
    initial_time_sql = TODO
    #my %initial_time = (
    #                "sec" => $current_time_sec,
    #                "sql" => $current_time_sql
    #              );
    
    #my @queues = OAR::IO::get_active_queues($base);
    #my $name;
    #my $policy;
    #foreach my $i (@queues){


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
                log.warn(e + " Cannot run: " + cmd_schedudler + " " + queue.name  + " " + 
                         initial_time_sec + " " + initial_time_sql)


            if ((!defined($sched_pid)) or ($sched_signal_num != 0) or ($sched_dumped_core != 0)){
            oar_error("[MetaSched] Error: execution of $policy failed, inactivating queue $name (see `oarnotify')\n");
            OAR::IO::stop_a_queue($base,$name);
            }

        if ($sched_exit_code == 1){
            $exit_code = 0;
        }elsif($sched_exit_code != 0){
            oar_error("[MetaSched] Error: Scheduler $policy returned a bad value: $sched_exit_code, at time $initial_time{sec}. Inactivating queue $policy (see `oarnotify')\n");
            OAR::IO::stop_a_queue($base,$name);
        }
        treate_waiting_reservation_jobs($name);
        check_reservation_jobs($name,$Order_part);
    }else{
        oar_debug("[MetaSched] Queue $name: No job\n");
    }
}

if ($exit_code == 0){
    if (check_jobs_to_kill() == 1){
        # We must kill besteffort jobs
        OAR::Tools::notify_tcp_socket($Remote_host,$Remote_port,"ChState");
        $exit_code = 2;
    }elsif (check_jobs_to_launch() == 1){
        $exit_code = 0;
    }
}

#Update visu gantt tables






    #Update visu gantt tables
    
    # Manage dynamic node feature
    
    # Send CHECK signal to Hulot if needed

    # Search jobs to resume
    
    # Notify oarsub -I when they will be launched
    
    # Run the decisions
    ## Treate "toError" jobs


## Treate toAckReservation jobs


## Treate toLaunch jobs
#foreach my $j (OAR::IO::get_jobs_in_state($base,"toLaunch")){
#    notify_to_run_job($base, $j->{job_id});
#}

return exit_code
