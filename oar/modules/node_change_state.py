#!/usr/bin/env python
# coding: utf-8
from oar.lib import (config, get_logger)
from oar.lib.event import (get_to_check_events, is_an_event_exists)
from oar.lib.job_handling import (get_job, get_job_types, set_job_state,
                                  is_job_already_resubmitted, resubmit_job)

logger = get_logger("oar.modules.node_change_state", forward_stderr=True)
logger.info('Start Note Change State')

#my $Remote_host = get_conf("SERVER_HOSTNAME");
#my $Remote_port = get_conf("SERVER_PORT");
#my $Cpuset_field = get_conf("JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD");
#my $Healing_exec_file = get_conf("SUSPECTED_HEALING_EXEC_FILE");
#my @resources_to_heal;

class  NodeChangeState(object):

    def __init__(self):
        pass

    def run(self):
        for event in get_to_check_events():
            job_id = event.job_id
            logger.debug('Check events for the job ' + str(job_id) + 'with type ' + event.type)
            job = get_job(job_id)

            # Check if we must resubmit the idempotent jobs
            # User must specify that his job is idempotent and exit from hos script with the exit code 99.
            # So, after a successful checkpoint, if the job is resubmitted then all will go right
            # and there will have no problem (like file creation, deletion, ...).
            if (((event.type == 'SWITCH_INTO_TERMINATE_STATE') or (event.type == 'SWITCH_INTO_ERROR_STATE'))
                and (job.exit_code and  (job.exit_code >> 8) == 99)):
                job_types = get_job_types(job_id)
                if 'idempotent' in job_types:
                    if ((job.reservation == 'None')
                        and (job.type == 'PASSIVE')
                        and (not is_job_already_resubmitted(job_id))
                        and (is_an_event_exists(job_id, 'SEND_KILL_JOB') == 0)
                        and ((job.stop_time - job.start_time) > 60)):

                        new_job_id = resubmit_job(job_id)
                        logger.warning('Resubmiting job ' + str(job_id) + ' => ' + str(new_job_id) +
                                       '(type idempotent & exit code = 99 & duration > 60s)')

            #  Check if we must expressely change the job state
            if (event.type == 'SWITCH_INTO_TERMINATE_STATE'):
                set_job_state(job_id, 'Terminated')
            elif  ((event.type == 'SWITCH_INTO_ERROR_STATE') or
                   (event.type == 'FORCE_TERMINATE_FINISHING_JOB')):
                set_job_state(job_id, 'Error')


            # Check if we must expressely change the job state #


            # Check if we must change the job state #

            
def main():
    node_change_state = NodeChangeState()
    node_change_state.run()

if __name__ == '__main__':  # pragma: no cover
    main()
