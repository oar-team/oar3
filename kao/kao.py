import os
import os.path as op
from oar import config, logging
import meta_sched


#import click

HERE = op.dirname(__file__)

def oar_debug(msg):
    print(msg)

def kao():

    oar_debug("[MetaSched] Starting Meta Scheduler\n");

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


#@click.command()
#@click.option('--count', default=1, help='Number of greetings.')
#@click.option('--name', prompt='Your name', help='The person to greet.')
#def kao(count, name):
#    """Simple program that greets NAME for a total of COUNT times."""
#    for x in range(count):
#        click.echo('Hello %s!' % name)

if __name__ == '__main__':
    kao()

