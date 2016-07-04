#!/usr/bin/env python
# coding: utf-8
"""Process that launches and manages bipbip processes
   OAREXEC_REGEXP  // 'OAREXEC_(\d+)_(\d+)_(\d+|N)_(\d+)'
   OARRUNJOB_REGEXP  // 'OARRUNJOB_(\d+)';
   LEONEXTERMINATE_REGEXP //  'LEONEXTERMINATE_(\d+)'
"""
from oar.lib import (config, get_logger)
from oar.lib.tools import call

import time
import zmq
from  multiprocessing import Process


# Set undefined config value to default one
DEFAULT_CONFIG = {
    'SERVER_HOSTNAME': 'localhost',
    'ZMQ_SERVER_PORT': '6667',
    'BIPBIP_COMMANDER_SERVER': 'localhost',
    'BIPBIP_COMMANDER_PORT': '6669',
    'MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING': '25',
    'DETACH_JOB_FROM_SERVER': '0',
    'LOG_FILE': '/var/log/oar.log'
}

config.setdefault_config(DEFAULT_CONFIG)

# Max number of concurrent bipbip processes
Max_bipbip_processes = config['MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING']
Detach_oarexec = config['DETACH_JOB_FROM_SERVER']

# Maximum duration a a bipbip process (after that time the process is killed)
Max_bipbip_process_duration = 30*60

logger = get_logger("oar.modules.bipbip_commander", forward_stderr=True)
logger.info('Start Bipbip Commander')

def bipbip_leon_executor(command, args):
    logger.debug('[bipbip_commander] Launching' + command['cmd'])
    call(command["cmd"].split(), shell=True)

def bipbip_commander():
    # Initialize a zeromq context
    context = zmq.Context()
    appendice = context.socket(zmq.PUSH) # signal Almighty
    appendice.bind('tcp://' + config['SERVER_HOSTNAME'] + ':' + config['ZMQ_SERVER_PORT'])

    notification = context.socket(zmq.PULL) # receive zmq formatted OAREXEC / OARRUNJOB / LEONEXTERMINATE
    notification.bind('tcp://' + config['BIPBIP_COMMANDER_SERVER'] + ':'
                      + config['BIPBIP_COMMANDER_SERVER'])

    bipbip_leon_commands_to_run = ()
    bipbip_leon_commands_to_requeue = ()
    bipbip_leon_executor = {}
    
    while True: # TODO: add a shutdown procedure
        #add_timeout if bipbip_leon_commands_to_run is not empty
        command = notification.recv_json()
        current = time.time()
        logger.debug("bipbip commander received notification:" + str(command))

        bipbip_leon_commands_to_run.append(command)
        
        while len(bipbip_leon_commands_to_run) > 0 and len(bipbip_leon_executors) <= Max_bipbip_processes:
            
            command = bipbip_leon_commands_to_run.pop(0)
            job_id = command['job_id']
            flag_exec = True
            
            if job_id in bipbip_leon_executor:
                if not bipbip_leon_executor[job_id].is_alive:
                     del bipbip_leon_executor[job_id]
                else
                flag_exec = False
                #requeue command
                logger.debug("[bipbip_commander] A process is already running for the job " +
                             job_id + ". We requeue: " + str(command))
                bipbip_leon_commands_to_requeue.append(command)
                
            if flag_exec:
                #exec
                executor = multiprocessing.Process(target= bipbip_leon_executor, args=(command))
                executor.start()
                bipbip_leon_executor[job_id] = executor

        # append commands to requeue
        bipbip_leon_commands_to_run += bipbip_leon_commands_to_requeue
        bipbip_leon_commands_to_requeue = ()
                
                
if __name__ == "__main__":
    bipbip_commander()

