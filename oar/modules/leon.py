#!/usr/bin/env python
# coding: utf-8

import sys
from oar.lib import (config, get_logger)
from oar.lib.job_handling import (get_job_frag_state, job_arm_leon_timer, job_finishing_sequence,
                                  get_jobs_to_kill, set_job_message, get_job_types,
                                  set_job_state, set_finish_date, get_job_current_hostnames,
                                  get_to_exterminate_jobs, set_running_date)
from oar.lib.event import add_new_event

import oar.lib.tools as tools

from oar.lib.tools import DEFAULT_CONFIG

logger = get_logger("oar.modules.leon", forward_stderr=True)
logger.info('Start Leon')


class Leon(object):

    def __init__(self):
        config.setdefault_config(DEFAULT_CONFIG)
        self.exit_code = 0
        
    def run(self, job_id):
        
        deploy_hostname = None
        if 'DEPLOY_HOSTNAME' in config:
            deploy_hostname = config['DEPLOY_HOSTNAME']

        cosystem_hostname = None
        
        if 'COSYSTEM_HOSTNAME' in config:
            cosystem_hostname = config['COSYSTEM_HOSTNAME']

        epilogue_script = config['SERVER_EPILOGUE_EXEC_FILE']
        openssh_cmd = config['OPENSSH_CMD']
        ssh_timeout = config['OAR_SSH_CONNECTION_TIMEOUT']
        oarexec_directory = config['OAR_RUNTIME_DIRECTORY']
        

        # Test if we must launch a finishing sequence on a specific job
        if sys.argv[0]:
            try:
                job_id = int(sys.argv[0])
            except ValueError as ex:
                logger.error('"%s" cannot be converted to an int' %  ex)
                self.exit_code = 1
                return
            
            frag_state = get_job_frag_state(job_id)

            if frag_state == 'LEON_EXTERMINATE':
                # TODO: from leon.pl, do we need to ignore some signals
                # $SIG{PIPE} = 'IGNORE';
                # $SIG{USR1} = 'IGNORE';
                # $SIG{INT}  = 'IGNORE';
                # $SIG{TERM} = 'IGNORE';
                logger.debug('Leon was called to exterminate job "' + str(job_id) + '"')
                job_arm_leon_timer(job_id)
                events = [('EXTERMINATE_JOB', 'I exterminate the job ' + str(job_id))]
                job_finishing_sequence(epilogue_script, job_id, events)
                tools.notify_almighty('ChState')
            else:
                logger.error('Leon was called to exterminate job "' + job_id +
                             '" but its frag_state is not LEON_EXTERMINATE')
                return

        for job in get_jobs_to_kill():
            #TODO pass if the job is job_desktop_computing one
            logger.debug('Normal kill: treates job ' + str(job.id))
            if (job.state == 'Wainting') or (job.state == 'Hold'):
                logger.debug('Job is not launched')
                set_job_state(job.id, 'Error')
                set_job_message(job_id, 'Job killed by Leon directly')
                if j.type == 'INTERACTIVE':
                    logger.debug('I notify oarsub in waiting mode')
                    addr, port = job.info_type.split(':')
                    if tools.notify_tcp_socket(addr, port, 'JOB_KILLED'):
                        logger.debug('Notification done')
                    else:
                         logger.debug('Cannot open connection to oarsub client for job '+
                                      str(job.job_id) +', it is normal if user typed Ctrl-C !')
                self.exit_code = 1
            elif  (job.state == 'Terminated') or (job.state == 'Error') or (job.state == 'Finishing'):
                logger.debug('Job is terminated or is terminating nothing to do')
            else:
                job_types = get_job_types(job.id)
                if 'noop' in job_types:
                    logger.debug('Kill the NOOP job: ' + str(job.id))
                    set_finish_date(job)
                    set_job_state(job, 'Terminated')
                    job_finishing_sequence(epilogue_script, job.id, [])
                    self.exit_code = 1
                else:
                    hosts = get_job_current_hostnames(job.id)
                    head_host = None
                    #deploy, cosystem and no host part
                    if ('cosystem' in job_types) or (len(hosts) == 0):
                        head_host = cosystem_hostname
                    elif 'deploy' in job_types:
                        head_host = deploy_hostname
                    elif len(hosts) != 0:
                        head_host = hosts[0]

                    if head_host:
                        add_new_event('SEND_KILL_JOB', job.id, 'Send the kill signal to oarexec on ' +
                                      head_host + ' for job ' +str(job.id))
                        tools.signal_oarexec(head_host, job.id, 'TERM', 0, openssh_cmd, '')
                    
            job_arm_leon_timer(job.id)
            
        # Treate jobs in state EXTERMINATED in the table fragJobs
        for job in get_to_exterminate_jobs():
            logger.debug('EXTERMINATE the job: ' + str(job.id))
            set_job_state(job.id, 'Finishing')
            if job.start_time == 0:
                set_running_date(job.id)
            set_finish_date(job)
            set_job_message(job_id, 'Job exterminated by Leon')
            tools.notify_almighty('LEONEXTERMINATE_' + str(job.id))

def main():
    leon = Leon()
    leon.run()
    return leon.exit_code

if __name__ == '__main__':  # pragma: no cover
    exit_code = main()
    sys.exit(exit_code)
