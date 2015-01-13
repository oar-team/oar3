import os
import sys
import subprocess
from oar.lib import config, Queue

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

def create_tcp_notification_socket():
    socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server = config["SERVER_HOSTNAME"]
    port =  config["SERVER_PORT"]
    try:
        socket.connect( (server, port) )
    except socket.error, exc:
        log.error("Connection to " + server + ":" + port + " raised exception socket.error: " + exc)
        sys.exit(1)
    return socket

def treate_waiting_reservation_jobs(name):
    pass

def check_reservation_jobs(name):
    pass

def check_jobs_to_kill():
    return 0

def check_jobs_to_launch():
    return 1


def update_gantt_visualization():
    sql_queries = ["TRUNCATE TABLE gantt_jobs_predictions_visu",
                   "TRUNCATE TABLE gantt_jobs_resources_visu",
                   "INSERT INTO gantt_jobs_predictions_visu SELECT * FROM gantt_jobs_predictions",
                   "INSERT INTO gantt_jobs_resources_visu SELECT * FROM gantt_jobs_resources"
                   ]
    for query in sql_queries:
        result = db.engine.execute(query)

def meta_schedule():

    exit_code = 0

    tcp_notify = create_tcp_notification_socket()

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
        tcp_notify.send("ChState")
        exit_code = 2
    elif check_jobs_to_launch() == 1:
        exit_code = 0

    #Update visu gantt tables
    update_gantt_visualization()

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

    log.debug("End of Meta Scheduler")

    return exit_code
