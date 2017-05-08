#!/usr/bin/env python
# coding: utf-8

from __future__ import unicode_literals, print_function
from oar.lib import (config, get_logger)
from oar.lib.tools import (Popen, PIPE)
import oar.lib.tools as tools

import socket
import zmq
import os
import re
import time
import signal
from pwd import getpwnam

import pdb

# Set undefined config value to default one
DEFAULT_CONFIG = {
    'META_SCHED_CMD': 'kao',
    'SERVER_HOSTNAME': 'localhost',
    'APPENDICE_SERVER_PORT': '6668', #new endpoint which replace appendic
    'SCHEDULER_MIN_TIME_BETWEEN_2_CALLS': '1',
    'FINAUD_FREQUENCY': '300',
    'LOG_FILE': '/var/log/oar.log',
    'ENERGY_SAVING_INTERNAL': 'no'
}

config.setdefault_config(DEFAULT_CONFIG)

# retrieve umask and set new one
old_umask = os.umask(0o022)

# TODO
# my $oldfh = select(STDERR); $| = 1; select($oldfh);
# $oldfh = select(STDOUT); $| = 1; select($oldfh);


# Everything is run by oar user (The real uid of this process.)
#os.environ['OARDO_UID'] = str(os.geteuid())

# TODO
# my $Redirect_STD_process = OAR::Modules::Judas::redirect_everything();

logger = get_logger("oar.modules.almighty", forward_stderr=True)
logger.info('Start Almighty')
# TODO
# send_log_by_email("Start OAR server","[Almighty] Start Almighty");

if 'OARDIR' in os.environ:
    binpath = os.environ['OARDIR'] + '/'
else:
    binpath = '/usr/local/lib/oar/'
    logger.warning("OARDIR env variable must be defined, " + binpath + " is used by default")

meta_sched_command = config['META_SCHED_CMD']
m = re.match(r'^\/', meta_sched_command)
if not m:
    meta_sched_command = binpath + meta_sched_command
    
leon_command = binpath + 'leon'
check_for_villains_command = binpath + 'sarko'
check_for_node_changes = binpath + 'finaud'
nodeChangeState_command = binpath + 'NodeChangeState'


# This timeout is used to slowdown the main automaton when the
# command queue is empty, it correspond to a blocking read of
# new commands. A High value is likely to reduce the CPU usage of
# the Almighty.
# Setting it to 0 or a low value is not likely to improve performance
# dramatically (because it blocks only when nothing else is to be done).
# Nevertheless it is closely related to the precision at which the
# internal counters are checked
read_commands_timeout = 10 * 1000 # in ms

# This parameter sets the number of pending commands read from
# appendice before proceeding with internal work
# should not be set at a too high value as this would make the
# Almighty weak against flooding
max_successive_read = 1;

# Max waiting time before new scheduling attempt (in the case of
# no notification)
schedulertimeout = 60
# Min waiting time before 2 scheduling attempts
scheduler_min_time_between_2_calls = int(config['SCHEDULER_MIN_TIME_BETWEEN_2_CALLS'])


# Max waiting time before check for jobs whose time allowed has elapsed
villainstimeout = 10

# Max waiting time before check node states
checknodestimeout = int(config['FINAUD_FREQUENCY'])

Log_file = config['LOG_FILE']

energy_pid = 0

# Signal handle
finishTag = False

def signal_handler():
    global finishTag
    finishTag = True

# To avoid zombie processes
#
signal.signal(signal.SIGUSR1, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def launch_command(command):
    '''launch the command line passed in parameter'''

    #TODO move to oar.lib.tools
    global finishTag

    logger.debug('Launching command : [' + command + ']')

    #import pdb; pdb.set_trace()
    
    status = tools.call(command)

    exit_value = status >> 8
    signal_num = status & 127
    dumped_core = status & 128

    logger.debug(command + ' terminated')
    logger.debug('Exit value : ' + str(exit_value))
    logger.debug('Signal num : ' + str(signal_num))
    logger.debug('Core dumped : ' + str(dumped_core))

    if signal_num or dumped_core:
        logger.error('Something wrong occured (signal or core dumped) when trying to call [' +
                     command + '] command')
        finishTag = 1

    return exit_value


def start_hulot():  # TODO
    '''hulot module forking'''
    # try:
    #    energy_pid = os.fork()
    #
    # if(!defined($energy_pid)){
    #    oar_error("[Almighty] Cannot fork Hulot, the energy saving module\n");
    #    exit(6);
    # }
    # if (!$energy_pid){
    #    $SIG{CHLD} = 'DEFAULT';
    #    $SIG{USR1}  = 'IGNORE';
    #    $SIG{INT}  = 'IGNORE';
    #    $SIG{TERM}  = 'IGNORE';
    #    $0="Almighty: hulot";
    #    OAR::Modules::Hulot::start_energy_loop();
    #    oar_error("[Almighty] Energy saving loop (hulot) exited. This should not happen.\n");
    #    exit(7);
    #  }
    # }


def check_hulot():
    '''check the hulot process'''
    return os.kill(energy_pid, 0)

# functions associated with each state of the automaton
def meta_scheduler():
    return launch_command(meta_sched_command)


def check_for_villains():
    return launch_command(check_for_villains_command)


def check_nodes():
    return launch_command(check_for_node_changes)


def leon():
    return launch_command(leon_command)


def nodeChangeState():
    return launch_command(nodeChangeState_command)


class Almighty(object):

    def __init__(self):  
        self.state = 'Init'
        logger.debug("Current state [" + self.state + "]")
        
        # Activate appendice socket
        self.context = zmq.Context()
        self.appendice = self.context.socket(zmq.PULL)
        ip_addr_server = socket.gethostbyname(config['SERVER_HOSTNAME'])
        try:
            self.appendice.bind('tcp://' + ip_addr_server + ':' + config['APPENDICE_SERVER_PORT'])
        except:
            logger.error('Failed to activate appendice endpoint')

        self.set_appendice_timeout(read_commands_timeout)
        
        # Starting of Hulot, the Energy saving module
        if config['ENERGY_SAVING_INTERNAL'] == 'yes':
            start_hulot()

        self.lastscheduler = 0
        self.lastvillains = 0
        self.lastchecknodes = 0
        self.command_queue = []

        self.scheduler_wanted = 0 # 1 if the scheduler must be run next time update

        logger.debug('Init done')
        self.state = 'Qget'

    def time_update(self):
        current = time.time()  # ---> TODO my $current = time; -> ???

        logger.debug('Timeouts check : ' + str(current))
        # check timeout for scheduler
        if (current >= (self.lastscheduler + schedulertimeout))\
           or (self.scheduler_wanted >= 1)\
           and (current >= (self.lastscheduler + scheduler_min_time_between_2_calls)):
            logger.debug('Scheduling timeout')
            # lastscheduler = current + schedulertimeout
            self.add_command('Scheduling')

        if current >= (self.lastvillains + villainstimeout):
            logger.debug('Villains check timeout')
            # lastvillains =  current +  villainstimeout
            self.add_command('Villains')

        if (current >= (self.lastchecknodes + checknodestimeout)) and (checknodestimeout > 0):
            logger.debug('Node check timeout')
            # lastchecknodes = -current + checknodestimeout
            self.add_command('Finaud')

    def set_appendice_timeout(self, timeout):
        '''Set timeout appendice socket'''
        self.appendice.RCVTIMEO = timeout


    def qget(self, timeout):
        '''function used by the main automaton to get notifications from appendice'''

        timeout = 10 * 1000
        self.set_appendice_timeout(timeout)

        logger.debug("Timeout value:" + str(timeout))
        
        try:
            answer = self.appendice.recv_json().decode('utf-8')
        except zmq.error.Again as e:
            logger.debug("Timeout from appendice:" + str(e))
            return {'cmd': 'Time'}
        except zmq.ZMQError as e:
            logger.error("Something is wrong with appendice" + str(e))
            exit(15)
        return answer

    def add_command(self, command):
        '''as commands are just notifications that will
        handle all the modifications in the base up to now, we should
        avoid duplication in the command file'''

        m = re.compile('^' + command + '$')
        flag = True
        for cmd in self.command_queue:
            if re.match(m, cmd):
                flag = False
                break

        if flag:
            self.command_queue.append(command)

    def read_commands(self, timeout):  # TODO
        ''' read commands until reaching the maximal successive read value or
        having read all of the pending commands'''

        command = None
        remaining = max_successive_read

        while (command != 'Time') and remaining:
            command = self.qget(timeout)
            if remaining != max_successive_read:
                timeout = 0
            if command is None:
                break
            self.add_command(command['cmd'])
            remaining -= 1
            logger.debug('Got command ' + command['cmd'] + ', ' + str(remaining) + ' remaining')

    def run(self, loop=True):
        
        global finishTag
        while True:
            logger.debug("Current state [" + self.state + "]")
            # We stop Almighty and its child
            if finishTag:
                if energy_pid:
                    logger.debug("kill child process " + str(energy_pid))
                    os.kill(energy_pid, signal.SIGKILL)
                # TODO:  $Redirect_STD_process = OAR::Modules::Judas::redirect_everything();
                Redirect_STD_process = False
                if Redirect_STD_process:
                    os.kill(Redirect_STD_process, signal.SIGKILL)
                ipc_clean()
                logger.warning("Stop Almighty\n")
                # TODO: send_log_by_email("Stop OAR server", "[Almighty] Stop Almighty")
                exit(10)

            # We check Hulot
            if energy_pid and not check_hulot():
                logger.warning("Energy saving module (hulot) died. Restarting it.")
                time.sleep(5)
                start_hulot()

            # QGET
            elif self.state == 'Qget':
                if len(self.command_queue) > 0:
                    self.read_commands(0)
                else:
                    self.read_commands(read_commands_timeout)

                logger.debug('Command queue : ' + str(self.command_queue))
                command = self.command_queue.pop(0)
                
                logger.debug('Qtype = [' + command + ']')
                if (command == 'Qsub') or (command == 'Term') or (command == 'BipBip')\
                   or (command == 'Scheduling') or (command == 'Qresume'):
                    self.state = 'Scheduler'
                elif command == 'Qdel':
                    self.state = 'Leon'
                elif command == 'Villains':
                    self.state = 'Check for villains'
                elif command == 'Finaud':
                    self.state = 'Check node states'
                elif command == 'Time':
                    self.state = 'Time update'
                elif command == 'ChState':
                    self.state = 'Change node state'
                else:
                    logger.error('Unknown command found in queue : ' + command)

            # SCHEDULER
            elif self.state == 'Scheduler':
                current_time = time.time()
                if current_time >= (self.lastscheduler + scheduler_min_time_between_2_calls):
                    self.scheduler_wanted = 0
                    # First, check pending events
                    check_result = nodeChangeState()
                    if check_result == 2:
                        self.state = 'Leon'
                        self.add_command('Term')
                    elif check_result == 1:
                        self.state = 'Scheduler'
                    elif check_result == 0:
                        # Launch the scheduler
                        # We check Hulot just before starting the scheduler
                        # because if the pipe is not read, it may freeze oar
                        if (energy_pid > 0) and not check_hulot():
                            logger.warning('Energy saving module (hulot) died. Restarting it.')
                            time.sleep(5)
                            start_hulot()

                        scheduler_result = meta_scheduler()
                        lastscheduler = time.time()
                        if scheduler_result == 0:
                            self.state = 'Time update'
                        elif scheduler_result == 1:
                            self.state = 'Scheduler'
                        elif scheduler_result == 2:
                            self.state = 'Leon'
                        else:
                            logger.error('Scheduler returned an unknown value : scheduler_result')
                            finishTag = 1

                    else:
                        logger.error('nodeChangeState_command returned an unknown value.')
                        finishTag = 1
                else:
                    self.scheduler_wanted = 1
                    self.state = 'Time update'
                    logger.debug('Scheduler call too early, waiting... (' + current_time +
                                 '>= (' + lastscheduler + ' + ' + scheduler_min_time_between_2_calls + ')')

            # TIME UPDATE
            elif self.state == 'Time update':
                self.time_update()
                self.state = 'Qget'

            # CHECK FOR VILLAINS
            elif self.state == 'Check for villains':
                check_result = check_for_villains()
                self.lastvillains = time.time()
                if check_result == 1:
                    self.state = 'Leon'
                elif check_result == 0:
                    self.state = 'Time update'
                else:
                    logger.error('check_for_villains_command returned an unknown value : check_result.')
                    finishTag = 1

            # CHECK NODE STATES
            elif self.state == 'Check node states':
                check_result = check_nodes()
                self.lastchecknodes = time.time()
                if check_result == 1:
                    self.state = 'Change node state'
                elif check_result == 0:
                    self.state = 'Time update'
                else:
                    logger.error('check_for_node_changes returned an unknown value.')
                    finishTag = 1

            # LEON
            elif self.state == 'Leon':
                check_result = leon()
                self.state = 'Time update'
                if check_result == 1:
                    self.add_command('Term')

            # Change state for dynamic nodes
            elif self.state == 'Change node state':
                check_result = nodeChangeState()
                if check_result == 2:
                    self.state = 'Leon'
                    self.add_command('Term')
                elif check_result == 1:
                    self.state = 'Scheduler'
                elif check_result == 0:
                    self.state = 'Time update'
                else:
                    logger.error('nodeChangeState_command returned an unknown value.')
                    finishTag = 1
            else:
                logger.warning('Critical bug !!!!\n')
                logger.error('Almighty just falled into an unknown state !!!.')
                finishTag = 1

            if not loop:
                break

if __name__ == '__main__':  # pragma: no cover
    almighty = Almighty()
    almighty.run()
