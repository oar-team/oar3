#!/usr/bin/env python
# coding: utf-8

from oar.lib import (config, get_logger)

from oar.lib.job_handling import (get_job, get_job_challenge, get_job_current_hostnames, check_end_of_job,
                                  get_current_moldable_job)
from oar.lib.tools import DEFAULT_CONFIG

from oar.lib.event import add_new_event

from oar.lib.tools import (Popen, TimeoutExpired)
                           
logger = get_logger("oar.modules.bipbip", forward_stderr=True)

class BipBip(object):

    def __init__(self, args):
        config.setdefault_config(DEFAULT_CONFIG)
        self.server_prologue = config['SERVER_PROLOGUE_EXEC_FILE']
        self.server_epilogue = config['SERVER_EPILOGUE_EXEC_FILE']

        self.exit_code = 0

        self.job_id = args[0]

        self.oarexec_reattach_exit_value = None
        self.oarexec_reattach_script_exit_value = None
        self.oarexec_challenge = None
        if len(args) >= 2:
            
            self.oarexec_reattach_exit_value = args[1]
        if len(args) >= 3:
            self.oarexec_reattach_script_exit_value = args[2]
        if len(args) >= 4:
            self.oarexec_challenge = args[3]
            
        set_ssh_timeout(congig[OAR_SSH_CONNECTION_TIMEOUT]) # TODO ???
        
    def run(self):
        
        job_id = self.job_id

        node_file_db_field = config['NODE_FILE_DB_FIELD']
        node_file_db_field_distinct_values = config['NODE_FILE_DB_FIELD_DISTINCT_VALUES']
        
        cpuset_field = config['JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD'] 

        cpuset_file = config['JOB_RESOURCE_MANAGER_FILE']
        if not re.match(r'^\/', cpuset_file):
            if 'OARDIR' not in os.environ:
                msg = '$OARDIR variable envionment must be defined'
                logger.error(msg)
                raise (msg)
            cpuset_file = os.environ['OARDIR'] + '/' + cpuset_file

        cpuset_path = config['CPUSET_PATH']
        cpuset_full_path = cpuset_path +'/' + cpuset_name
        
        job_challenge, ssh_private_key, ssh_public_key = get_job_challenge(job_id)
        hosts = get_job_current_hostnames(job_id)
        job = get_job(job_id)
        
        #Check if we must treate the end of a oarexec
        if self.oarexec_reattach_exit_value and job.state in ['Launching', 'Running', 'Suspended', 'Resuming']:
            logger.debug('[' + str(job.id) + '] OAREXEC end: ' + self.oarexec_reattach_exit_value\
                         + ' ' + self.oarexec_reattach_script_exit_value)
            
            try: 
                int(self.oarexec_reattach_exit_value)
                
            except ValueError:
                logger.error('[' + str(job.id) + '] Bad argument for bipbip : ' + self.oarexec_reattach_exit_value)
                self.exit_code = 2
                return

            if self.oarexec_challenge == job_challenge:
                check_end_of_job(job_id, self.oarexec_reattach_script_exit_value, self.oarexec_reattach_exit_value,
                                 hosts, job.user, job.launching_directory, self.server_epilogue)
                return
            else:
                msg =  'Bad challenge from oarexec, perhaps a pirate attack??? ('\
                       + self.oarexec_challenge + '/' + job_challenge + ').'
                logger.error('[' + str(job.id) + '] ' + msg)
                add_new_event('BIPBIP_CHALLENGE', job_id, msg)
                self.exit_code = 2
                return

        if job.state == 'toLaunch':
            # Tell that the launching process is initiated
            set_job_state(job_id, 'Launching')
            job.state = 'Launching'
        else:
            logger.warning('[' + str(job.id) + '] Job already treated or deleted in the meantime')
            self.exit_code = 1
            return
        
        resources = get_current_assigned_job_resource(job.assigned_moldable_job)
        mold_job_description = get_current_moldable_job(job.assigned_moldable_job)
            
        # NOOP jobs
        job_types = get_job_types(job.id)
        if 'noop' in job_types:
            set_job_state(job_id, 'Running')
            logger.debug('[' + str(job.id) + '] User: ' + job.user + ' Set NOOP job to Running')
            call_server_prologue()
            return
            
        # HERE we must launch oarexec on the first node
        logger.debug('[' + str(job.id) + '] User: ' + job.user + '; Command: ' + job.command\
                      + ' ==> hosts : ' + str(hosts))

        if (job.type == 'INTERACTIVE') and (job.reservation == 'None'):
            tools.notify_interactif_user(job, 'Starting...')

        if ('deploy' not in job_types) and ('cosystem' not in job_types) and (len(hosts) > 0):
            ###############
            # CPUSET PART #
            ###############
            nodes_cpuset_fields = None
            if cpuset_field:
                nodes_cpuset_fields = get_cpuset_values(self.cpuset_field, job.assigned_moldable_job)
                
            ssh_public_key = format_ssh_pub_key(ssh_public_key, cpuset_full_path, job.user, job.user)
            
            cpuset_data_hash = {
                'job_id': job.id,
                'name': cpuset_name,
                'nodes': cpuset_nodes,
                'cpuset_path': cpuset_path,
                'ssh_keys': {
                    'public': {
                        'file_name': config['OAR_SSH_AUTHORIZED_KEYS_FILE'],
                        'key': ssh_public_key
                    },
                    'private': {
                        'file_name': get_private_ssh_key_file_name(cpuset_name),
                        'key': ssh_private_key
                    },
                },
                'oar_tmp_directory': config['OAREXEC_DIRECTORY'],
                
                'user': job_user,
                'job_user': job_user,
                'types': job_types,
                'resources': resources,
                'node_file_db_fields': node_file_db_field,
                'node_file_db_fields_distinct_values': node_file_db_field_distinct_values,
                'array_id': job.array_id,
                'array_index': job.array_index,
                'stdout_file': job.stdout_file.replace('%jobid%', str(job.id)),
                'stderr_file': job.stderr_file.replace('%jobid%', str(job.id)),
                'launching_directory': job.launching_directory,
                'job_name': job.name,
                'walltime_seconds': 'undef',
                'walltime': 'undef',
                'project': job.project,
                'log_level': config['LOG_LEVEL']
            }

            if len(nodes_cpuset_fields) > 0:
                taktuk_cmd = config['TAKTUK_CMD']
                cpuset_data_str = limited_dict2hash_perl(cpuset_data_hash)
                tag, bad = tools.manage_remote_commands(nodes_cpuset_fields.keys(),
                                                        cpuset_data_str, cpuset_file,
                                                        'init', openssh_cmd, taktuk_cmd)
            if tag == 0:
                msg = '[JOB INITIATING SEQUENCE] [CPUSET] [' + str(job.id)\
                      + '] Bad cpuset file: ' + cpuset_file
                logger.error(msg)
                events.append(('CPUSET_MANAGER_FILE', msg, None))
            elif len(bad) > 0:
                event_type = 'CPUSET_ERROR'
                # Clean already configured cpuset
                
            #####################
            # CPUSET PART, END  #
            #####################

            
    def call_server_prologue(job):
        # PROLOGUE EXECUTED ON OAR SERVER #
        # Script is executing with job id in arguments
        if self.server_prologue:
            timeout = config['SERVER_PROLOGUE_EPILOGUE_TIMEOUT']
            cmd = [self.server_prologue, str(job.id)]

            try:
                child = Popen(cmd)
                return_code = child.wait(timeout)

                if return_code:
                    msg = '[' + str(job.id) + '] Server prologue exit code: ' + str(return_code)\
                          + ' (!=0) (cmd: ' + str(cmd) + ')'
                    logger.error(msg)
                    add_new_event('SERVER_PROLOGUE_EXIT_CODE_ERROR', job.id, '[bipbip] ' + msg)
                    tools.notify_almighty('ChState')
                    if (job.type == 'INTERACTIVE') and (job.reservation == 'None'):
                        tools.notify_interactif_user(job, 'ERROR: SERVER PROLOGUE returned a bad value')
                    self.exit_code = 2
                    return 1
                
            except OSError as e:   
                logger.error('Cannot run: ' + str(cmd))
                
            except TimeoutExpired as e:
                tools.kill_child_processes(child.pid)
                msg = '[' + str(job.id) + '] Server prologue timeouted (cmd: ' + str(cmd)
                logger.error(msg)
                add_new_event('SERVER_PROLOGUE_TIMEOUT', job.id, '[bipbip] ' + msg)
                tools.notify_almighty('ChState')
                if (job.type == 'INTERACTIVE') and (job.reservation == 'None'):
                    tools.notify_interactif_user(job, 'ERROR: SERVER PROLOGUE timeouted')
                self.exit_code = 2
                return 1
            
            return 0
            
            
def main():
    bipbip = BipBip()
    bipbip.run()
    return bipbip.exit_code
    
if __name__ == '__main__':  # pragma: no cover
    if len(sys.argv) < 2:
        # TODO
        sys.exit(1)
        
    exit_code = main(sys.argv[1:])
    sys.exit(exit_code)
