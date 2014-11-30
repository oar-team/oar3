
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
