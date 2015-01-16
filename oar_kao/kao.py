from oar.lib import get_logger
from oar.kao.meta_sched import meta_schedule

log = get_logger("oar.kao")
#import click

def kao():

    log.info("[MetaSched] Starting Meta Scheduler");
    meta_schedule()

    ###########################################################################################################
    #Initialize Gantt tables with scheduled reservation jobs, Running jobs, toLaunch jobs and Launching jobs; #
    ###########################################################################################################

    # First get resources that we must add for each reservation jobs
    # TODO move to admission rules ???

    # Take care of the currently (or nearly) running jobs
    # Lock to prevent bipbip update in same time
    # TODO: do we need to locks table ????
    #OAR::IO::lock_table($base,["jobs","assigned_resources","gantt_jobs_predictions","gantt_jobs_resources","job_types","moldable_job_descriptions","resources","job_state_logs","gantt_jobs_predictions_log","gantt_jobs_resources_log"])


    #calculate now date with no overlap with other jobs

if __name__ == '__main__':
    kao()

