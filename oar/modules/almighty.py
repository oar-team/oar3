#!/usr/bin/env python
# coding: utf-8

from __future__ import unicode_literals, print_function
from oar.lib import (config, get_logger)
from oar.lib.tools import (Popen, PIPE)

import zmq
import os
import re
import time
import signal
from pwd import getpwnam


# Set undefined config value to default one
DEFAULT_CONFIG = {
    'META_SCHED_CMD': 'kao',
    'SERVER_HOSTNAME': 'localhost',
    'ZMQ_SERVER_PORT': '6667', # new endpoint which replace appendice
    'APPENDICE_PROXY_SERVER_PORT': '6668', # endpoint for appendice proxy
    'SCHEDULER_MIN_TIME_BETWEEN_2_CALLS': '1',
    'FINAUD_FREQUENCY': '300',
    'MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING': '25',
    'DETACH_JOB_FROM_SERVER': '0',
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
os.environ['OARDO_UID'] = str(os.geteuid())

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

# Signal handle
finishTag = False


def signal_handler():
    global finishTag
    finishTag = True


# To avoid zombie processes
#
# Surely useless, ignoring SIGCHLD is the common default setting
signal.signal(signal.SIGCHLD, signal.SIG_IGN)

signal.signal(signal.SIGUSR1, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

meta_sched_command = config['META_SCHED_CMD']
m = re.match(r'^\/', meta_sched_command)
if not m:
    meta_sched_command = binpath + meta_sched_command
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
read_commands_timeout = 10

# This parameter sets the number of pending commands read from
# appendice before proceeding with internal work
# should not be set at a too high value as this would make the
# Almighty weak against flooding
max_successive_read = 100;

# Max waiting time before new scheduling attempt (in the case of
# no notification)
schedulertimeout = 60
# Min waiting time before 2 scheduling attempts
scheduler_min_time_between_2_calls = config['SCHEDULER_MIN_TIME_BETWEEN_2_CALLS']
scheduler_wanted = 0  # 1 if the scheduler must be run next time update

# Max waiting time before check for jobs whose time allowed has elapsed
villainstimeout = 10

# Max waiting time before check node states
checknodestimeout = config['FINAUD_FREQUENCY']

# Max number of concurrent bipbip processes
Max_bipbip_processes = config['MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING']
Detach_oarexec = config['DETACH_JOB_FROM_SERVER']

# Maximum duration a a bipbip process (after that time the process is killed)
Max_bipbip_process_duration = 30*60

Log_file = config['LOG_FILE']

# Regexp of the notification received from oarexec processes
#   $1: job id
#   $2: oarexec exit code
#   $3: job script exit code
#   $4: secret string that identifies the oarexec process (for security)
OAREXEC_REGEXP = r'OAREXEC_(\d+)_(\d+)_(\d+|N)_(\d+)'

# Regexp of the notification received when a job must be launched
#   $1: job id
OARRUNJOB_REGEXP = r'OARRUNJOB_(\d+)'

# Regexp of the notification received when a job must be exterminate
#   $1: job id
LEONEXTERMINATE_REGEXP = r'LEONEXTERMINATE_(\d+)'

energy_pid = 0


def launch_command(command):
    '''launch the command line passed in parameter'''

    #TODO move to oar.lib.tools
    global finishTag

    logger.debug('Launching command : [' + command + ']')
    # $ENV{PATH}="/bin:/usr/bin:/usr/local/bin";
    #  ###### THE LINE BELOW SHOULD NOT BE COMMENTED IN NORMAL USE ##### ??? TODO (BELOW -> ABOVE ???)
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    # system $command; ??? TODO  to remove ?

    pid = os.fork()
    if pid == 0:
        # CHILD
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        os.execv(command, ['Almighty: ' + command])

    status = 0
    while True:
        kid, status = os.wait()
        if kid == pid:
            break
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    exit_value = status >> 8
    signal_num = status & 127
    dumped_core = status & 128

    logger.debug(command + ' terminated')
    logger.debug('Exit value : ' + exit_value)
    logger.debug('Signal num : ' + signal_num)
    logger.debug('Core dumped : ' + dumped_core)

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


def ipc_clean():  # TODO do we need it ?
    '''Clean ipcs'''
    oar_uid = getpwnam('oar').pw_uid
    with open('/proc/sysvipc/msg') as f_ipcs:
        for line in f_ipcs:
            ipcs = line.slip()
            if int(ipcs[7]) == oar_uid:
                logger.debug('cleaning ipc ' + ipcs[7])
                os.system('/usr/bin/ipcrm -q ' + ipcs[7])


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
        try:
            self.appendice.bind('tcp://' + config['SERVER_HOSTNAME'] + ':' + config['SERVER_PORT'])
        except:
            logger.error('Failed to activate appendice endpoint')

        self.set_appendice_timeout(read_commands_timeout)

        self.appendice_proxy = self.context.socket(zmq.PULL)
        try:
            self.appendice_proxy.bind('tcp://' + config['SERVER_HOSTNAME'] + ':'
                                      + config['APPENDICE_PROXY_SERVER_PORT'])
        except:
            logger.error('Failed to activate appendice proxy endpoint')
        
        # Starting of Hulot, the Energy saving module
        if config['ENERGY_SAVING_INTERNAL'] == 'yes':
            start_hulot()

        self.lastscheduler = 0
        self.lastvillains = 0
        self.lastchecknodes = 0
        self.internal_command_file = []

        logger.debug('Init done')
        self.state = 'Qget'

    def time_update(self):
        current = time.time()  # ---> TODO my $current = time; -> ???

        logger.debug('Timeouts check : ' + str(current))
        # check timeout for scheduler
        if (current >= (self.lastscheduler + schedulertimeout))\
           or (scheduler_wanted >= 1)\
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

        self. set_appendice_timeout(timeout)

        answer = None

        try:
            answer = self.appendice.recv_json()
        except zmq.error.Again as e:
            logger.debug("Timeout from appendice:" + str(e))
            return "Time"
        except zmq.ZMQError as e:
            logger.error("Something is worng with appendice" + str(e))
            exit(15)
        return answer['cmd']

    def add_command(self, command):
        '''as commands are just notifications that will
        handle all the modifications in the base up to now, we should
        avoid duplication in the command file'''

        m = re.compile('^' + command + '$')
        flag = True
        for cmd in self.internal_command_file:
            if re.match(m, cmd):
                flag = False
                break

        if flag:
            self.internal_command_file += command

    def read_commands(self, timeout):  # TODO
        ''' read commands until reaching the maximal successive read value or
        having read all of the pending commands'''

        command = ''
        remaining = max_successive_read

        while (command != 'Time') and remaining:
            if remaining != max_successive_read:
                timeout = 0
            command = self.qget(timeout)
            self.add_command(command)
            remaining -= 1
            logger.debug('Got command ' + command + ', ' + str(remaining) + ' remaining')

    def run(self):

        global finishTag
        while True:
            logger.debug("Current state [" + self.state + "]")
            # We stop Almighty and its child
            if finishTag:
                if energy_pid:
                    logger.debug("kill child process " + str(energy_pid))
                    os.kill(energy_pid, signal.SIGKILL)
                logger.debug("kill child process " + str(self.appendice_pid))
                os.kill(self.appendice_pid, signal.SIGKILL)
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
                ipc_clean()
                start_hulot()

            # QGET
            elif self.state == 'Qget':
                if self.internal_command_file != []:
                    self.read_commands(0)
                else:
                    self.read_commands(read_commands_timeout)

                logger.debug('Command queue : ' + str(self.internal_command_file))
                current_command = self.internal_command_file.pop(0)
                command, arg1, arg2, arg3 = re.split(' ', current_command)

                logger.debug('Qtype = [' + command + ']')
                if (command == 'Qsub') or (command == 'Term') or (command == 'BipBip')\
                   or (command == 'Scheduling') or (command == 'Qresume'):
                    self.state = 'Scheduler'
                elif command == 'Qdel':
                    self.state == 'Leon'
                elif command == 'Villains':
                    self.state = 'Check for villains'
                elif command == 'Finaud':
                    self.state = 'Check node states'
                elif command == 'Time':
                    self.state = 'Time update'
                elif command == 'ChState':
                    self.state = 'Change node state'
                else:
                    logger.debug('Unknown command found in queue : ' + command)

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
                            ipc_clean()
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


def main():
    almighty = Almighty()
    almighty.run()

if __name__ == '__main__':  # pragma: no cover
    main()
