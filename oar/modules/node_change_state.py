#!/usr/bin/env python
# coding: utf-8
from oar.lib import (config, get_logger)
from oar.lib.event import (get_to_check_events, is_an_event_exists)
from oar.lib.job_handling import (get_job, get_job_types, set_job_state,
                                  is_job_already_resubmitted, resubmit_job)

logger = get_logger("oar.modules.node_change_state", forward_stderr=True)
logger.info('Start Note Change State')

class  NodeChangeState(object):

    def __init__(self):
        self.resources_to_heal = []
        self.cpuset_field = None
        if 'JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD' in config:
            self.cpuset_field = config['JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD']
        self.healing_exec_file = None
        if 'SUSPECTED_HEALING_EXEC_FILE' in config:
           self.healing_exec_file = config['SUSPECTED_HEALING_EXEC_FILE']
        
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
                    if (job.reservation == 'None'
                        and job.type == 'PASSIVE'
                        and (not is_job_already_resubmitted(job_id))
                        and (is_an_event_exists(job_id, 'SEND_KILL_JOB') == 0)
                        and ((job.stop_time - job.start_time) > 60)):
                        
                        new_job_id = resubmit_job(job_id)
                        logger.warning('Resubmiting job ' + str(job_id) + ' => ' + str(new_job_id) +
                                       '(type idempotent & exit code = 99 & duration > 60s)')

            #  Check if we must expressely change the job state
            if event.type == 'SWITCH_INTO_TERMINATE_STATE':
                set_job_state(job_id, 'Terminated')

            elif (event.type == 'SWITCH_INTO_ERROR_STATE') or (event.type == 'FORCE_TERMINATE_FINISHING_JOB'):
                set_job_state(job_id, 'Error')

            # Check if we must change the job state #
            type_to_check = ['PING_CHECKER_NODE_SUSPECTED', 'CPUSET_ERROR', 'PROLOGUE_ERROR',
                             'CANNOT_WRITE_NODE_FILE', 'CANNOT_WRITE_PID_FILE', 'USER_SHELL',
                             'EXTERMINATE_JOB', 'CANNOT_CREATE_TMP_DIRECTORY', 'LAUNCHING_OAREXEC_TIMEOUT',
                             'RESERVATION_NO_NODE', 'BAD_HASHTABLE_DUMP', 'SSH_TRANSFER_TIMEOUT',
                             'EXIT_VALUE_OAREXEC']

            if event.type in type_to_check:
                if ((job.reservation == 'None') or (event.type == 'RESERVATION_NO_NODE')
                    or (job.assigned_moldable_job == 0)):
                    set_job_state(job_id, 'Error')
                elif (job.reservation and (event.type != 'PING_CHECKER_NODE_SUSPECTED')
                      and (event.type != 'CPUSET_ERROR')):
                        set_job_state(job_id, 'Error')

            if (event.type == 'CPUSET_CLEAN_ERROR') or (event.type == 'EPILOGUE_ERROR'):
                # At this point the job was executed normally
                # The state is changed here to avoid to schedule other jobs
                # on nodes that will be Suspected
                set_job_state(job_id, 'Terminated')
                        
            # Check if we must suspect some nodes
            type_to_check = ['PING_CHECKER_NODE_SUSPECTED', 'PING_CHECKER_NODE_SUSPECTED_END_JOB',
                             'CPUSET_ERROR', 'CPUSET_CLEAN_ERROR', 'SUSPEND_ERROR',
                             'RESUME_ERROR', 'PROLOGUE_ERROR', 'EPILOGUE_ERROR',
                             'CANNOT_WRITE_NODE_FILE', 'CANNOT_WRITE_PID_FILE',
                             'USER_SHELL', 'EXTERMINATE_JOB', 'CANNOT_CREATE_TMP_DIRECTORY',
                             'SSH_TRANSFER_TIMEOUT', 'BAD_HASHTABLE_DUMP',
                             'LAUNCHING_OAREXEC_TIMEOUT', 'EXIT_VALUE_OAREXEC',
                             'FORCE_TERMINATE_FINISHING_JOB']
            type_to_check_cpuset_SR_error = ['CPUSET_ERROR', 'CPUSET_CLEAN_ERROR', 'SUSPEND_ERROR',
                                             'RESUME_ERROR',  ]
            type_to_check_cpuset_LT_error = ['EXTERMINATE_JOB', 'PROLOGUE_ERROR', 'EPILOGUE_ERROR',
                                             'CPUSET_ERROR', 'CPUSET_CLEAN_ERROR',
                                             'FORCE_TERMINATE_FINISHING_JOB']
             
            if event.type in type_to_check:
                hosts = []
                finaud_tag = 'NO'
                # Restrict Suspected state to the first node (node really connected with OAR)
                # for some event types
                if (event.type == 'PING_CHECKER_NODE_SUSPECTED'
                    or event.type == 'PING_CHECKER_NODE_SUSPECTED_END_JOB'):
                    hosts = get_hostname_event(event.event_id)
                    finaud_tag = 'YES'
                elif event.type in type_to_check_cpuset_SR_error: 
                    hosts = get_hostname_event(event.event_id)
                else:
                    hosts = get_job_host_log(job.assigned_moldable_job)
                    if event.type not in type_to_check_cpuset_LT_error:
                        hosts = [hosts[0]]
                    else:
                        # If we exterminate a job and the cpuset feature is configured
                        # then the CPUSET clean will tell us which nodes are dead
                        if (('JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD' in config)
                            and (event.type == 'EXTERMINATE_JOB')):
                            hosts = []
                    add_new_event_with_host('LOG_SUSPECTED', 0, event.description, hosts)
                        
            if len(hosts) > 0:
                already_treated_hosts = {}
                for host in hosts:
                    if not ((host in already_treated_hosts) or (host == '')):
                        already_treated_hosts[host] = True
                        set_node_state(host, 'Suspected', finaud_tag)
                        for resource in get_all_resources_on_node(host):
                            self.resources_to_heal.append(str(resource_id) + ' ' + host)
                        exit_code = 1
                msg = 'Set nodes to suspected after error' + event.type + ' ' + ','.join(hosts))
                logger.warning(msg)
                tools.send_log_by_email('Suspecting nodes', msg)
                
            # Check if we must stop the scheduling
            type_to_check = ['SERVER_PROLOGUE_TIMEOUT', 'SERVER_PROLOGUE_EXIT_CODE_ERROR',
                             'SERVER_EPILOGUE_TIMEOUT', 'SERVER_EPILOGUE_EXIT_CODE_ERROR']
            if event.type in type_to_check:
                logger.warning('Server admin script error, stopping all scheduler queues: ' + event.type)
                tools.send_log_by_email('Stop all scheduling queues',
                                        'Server admin script error, stopping all scheduler queues: ' +
                                         event.type + ". Fix errors and run `oarnotify -E' to re-enable them.")
                stop_all_queues()
                set_job_state(job_id, 'Error')
                
            # Check if we must resubmit the job
            type_to_check = ['SERVER_PROLOGUE_TIMEOUT', 'SERVER_PROLOGUE_EXIT_CODE_ERROR',
                             'SERVER_EPILOGUE_TIMEOUT', 'PING_CHECKER_NODE_SUSPECTED',
                             'CPUSET_ERROR', 'PROLOGUE_ERROR', 'CANNOT_WRITE_NODE_FILE',
                             'CANNOT_WRITE_PID_FILE', 'USER_SHELL',
                             'CANNOT_CREATE_TMP_DIRECTORY', 'LAUNCHING_OAREXEC_TIMEOUT']
            if event.type in type_to_check:
                if (job.reservation == 'None' and job.type == 'PASSIVE'
                    and (is_job_already_resubmitted(job_id) == 0)):
                    new_job_id = resubmit_job(job_id)
                    msg =  'Resubmiting job ' + str(job_id) + ' => ' + str(new_job_id) +\
                           ' (due to event ' + event.type +\
                           ' & job is neither a reservation nor an interactive job)'
                    logger.warning(msg)
                    add_new_event('"RESUBMIT_JOB_AUTOMATICALLY', job_id, msg)

            # Check Suspend/Resume job feature
            if event.type in ['HOLD_WAITING_JOB', 'HOLD_RUNNING_JOB', 'RESUME_JOB']:
                
                if event.type != 'RESUME_JOB' and job.state == 'Waiting':
                    set_job_state(job_id, 'Hold')
                    if job.type == 'INTERACTIVE':
                        addr, port = job.info_type.split(':')
                        tools.notify_tcp_socket(addr,port,'Start prediction: undefined (Hold)')
                        
                elif event.type != 'RESUME_JOB' and job.state == 'Resuming':
                    set_job_state(job_id, 'Suspended')
                    tools.notify_almighty('Term')
                    
                elif event.type == 'HOLD_WAITING_JOB' and job.state == 'Running':
                    job_types = get_job_types(job_id)
                    if 'noop' in job_types:
                        suspend_job_action(job_id, job.assigned_moldable_job)
                        logger.debug(str(job_id) + ' suspend job of type noop')
                        tools.notify_almighty('Term')
                    else:
                        # Launch suspend command on all nodes
                        self.suspend_job(job, event)
                        
                elif event.type == 'RESUME_JOB' and job.state == 'Suspend':
                    set_job_state(job_id, 'Resuming')
                    tools.notify_almighty('Qresume')
                    
                elif event.type == 'RESUME_JOB' and job.state == 'Hold':
                    set_job_state(job_id, 'Waiting')
                    tools.notify_almighty('Qresume')
                    
            # Check if we must notify the user
            if event.type == 'FRAG_JOB_REQUEST':
                raise NotImplementedError('judas_notify_user')
                # my ($addr,$port) = split(/:/,$job->{info_type});
                # OAR::Modules::Judas::notify_user($base,$job->{notify},$addr,$job->{job_user},$job->{job_id},$job->{job_name},"INFO","Your job was asked to be deleted - $i->{description}");}
                #addr, port = job.info_type.split(':')
                #tools.judas_notify_user(job.notify, addr, job.user, job_idn port,'Start prediction: undefined (Hold)')
                
            check_event(event.type, job_id)

            
        # Treate nextState field
            
    def suspend_job(self, job, event):
        # SUSPEND PART
        
        if self.cpuset_field:
            cpuset_name = get_job_cpuset_name(job.id)
            cpuset_nodes = get_cpuset_values_for_a_moldable_job(self.cpuset_field, job.assigned_moldable_job)
            if cpuset_nodes:
                #TODO taktuk command
                raise NotImplementedError('taktuk command for suspend part')

        
def main():
    node_change_state = NodeChangeState()
    node_change_state.run()

if __name__ == '__main__':  # pragma: no cover
    main()
