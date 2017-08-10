#!/usr/bin/env python
# coding: utf-8
"""Process that launches and manages bipbip processes
   OAREXEC_REGEXP   OAREXEC_(\d+)_(\d+)_(\d+|N)_(\d+)
   OARRUNJOB_REGEXP   OARRUNJOB_(\d+)
   LEONEXTERMINATE_REGEXP   'LEONEXTERMINATE_(\d+)

   Commands:
     OAREXEC
     OARRUNJOB
     LEONEXTERMINATE


 TODO: Doit ton 


   Example
{
   "job_id": 5,
   "cmd": "LEONEXTERMINATE"
   "args": [5]
}

"""

from multiprocessing import Process

import os
import socket
import zmq

from oar.lib import (config, get_logger)
import oar.lib.tools as tools

# Set undefined config value to default one
DEFAULT_CONFIG = {
    'SERVER_HOSTNAME': 'localhost',
    'APPENDICE_SERVER_PORT': '6668',
    'BIPBIP_COMMANDER_SERVER': 'localhost',
    'BIPBIP_COMMANDER_PORT': '6669',
    'MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING': '25',
    'DETACH_JOB_FROM_SERVER': '1',
    'LOG_FILE': '/var/log/oar.log'
}

config.setdefault_config(DEFAULT_CONFIG)

# Max number of concurrent bipbip processes
Max_bipbip_processes = int(config['MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING'])
Detach_oarexec = config['DETACH_JOB_FROM_SERVER']

# Maximum duration a a bipbip process (after that time the process is killed)
Max_bipbip_process_duration = 30*60

logger = get_logger("oar.modules.bipbip_commander", forward_stderr=True)
logger.info('Start Bipbip Commander')

if 'OARDIR' in os.environ:
    binpath = os.environ['OARDIR'] + '/'
else:
    binpath = '/usr/local/lib/oar/'
    os.environ['OARDIR'] = binpath
    logger.warning("OARDIR env variable must be defined, " + binpath + " is used by default")

leon_command = binpath + 'leon'
bipbip_command = binpath + 'bipbip'


def bipbip_leon_executor(*args, **command):
    
    job_id = command['job_id']
    
    if command['cmd'] == 'LEONEXTERMINATE':
        cmd_arg = [leon_command, str(job_id)]
    else:
        cmd_arg = [bipbip_command, str(job_id)] + command['args']

    logger.debug('Launching: ' + str(cmd_arg))
            
    # TODO returncode,
    tools.call(cmd_arg)

class BipbipCommander(object):
    
    def __init__(self):
        # Initialize zeromq context
        self.context = zmq.Context()

        # TODO signal Almighty
        #self.appendice = self.context.socket(zmq.PUSH) # to signal Almighty
        #self.appendice.connect('tcp://' + config['SERVER_HOSTNAME'] + ':' + config['APPENDICE_SERVER_PORT'])


        # IP addr is required when bind function is used on zmq socket
        ip_addr_bipbip_commander = socket.gethostbyname(config['BIPBIP_COMMANDER_SERVER'])
        self.notification = self.context.socket(zmq.PULL) # receive zmq formatted OAREXEC / OARRUNJOB / LEONEXTERMINATE
        self.notification.bind('tcp://' + ip_addr_bipbip_commander + ':' + config['BIPBIP_COMMANDER_PORT'])
        
        self.bipbip_leon_commands_to_run = []
        self.bipbip_leon_commands_to_requeue = []
        self.bipbip_leon_executors = {}

    
    def run(self, loop=True):
        # TODO: add a shutdown procedure
        while True:
            #add_timeout if bipbip_leon_commands_to_run is not empty
            command = self.notification.recv_json().decode('utf-8')

            logger.debug("bipbip commander received notification:" + str(command))
            #import pdb; pdb.set_trace()
            self.bipbip_leon_commands_to_run.append(command)

            while len(self.bipbip_leon_commands_to_run) > 0 and \
                  len(self.bipbip_leon_executors.keys()) <= Max_bipbip_processes:

                command = self.bipbip_leon_commands_to_run.pop(0)
                job_id = command['job_id']
                flag_exec = True

                if job_id in self.bipbip_leon_executors:
                    if not self.bipbip_leon_executors[job_id].is_alive():
                        del self.bipbip_leon_executors[job_id]
                    else:
                        flag_exec = False
                        # requeue command
                        logger.debug("A process is already running for the job " +
                                     str(job_id) + ". We requeue: " + str(command))
                        self.bipbip_leon_commands_to_requeue.append(command)

                if flag_exec:
                    # exec
                    executor = Process(target=bipbip_leon_executor, args=(), kwargs=command)
                    executor.start()
                    self.bipbip_leon_executors[job_id] = executor

            # append commands to requeue
            self.bipbip_leon_commands_to_run += self.bipbip_leon_commands_to_requeue
            self.bipbip_leon_commands_to_requeue = []


            # Remove finished executors:
            for job_id in self.bipbip_leon_executors.keys():
                if not self.bipbip_leon_executors[job_id].is_alive():
                    del self.bipbip_leon_executors[job_id]
            
            if not loop:
                break


if __name__ == "__main__":
    bipbip_commander = BipbipCommander()
    bipbip_commander.run()

