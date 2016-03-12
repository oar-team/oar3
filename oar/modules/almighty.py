#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals, print_function
from oar.lib import (config, get_logger)
import os
import re
import time
import signal


# Set undefined config value to default one
DEFAULT_CONFIG = {
    'META_SCHED_CMD' = 'kao',
    'SERVER_HOSTNAME' = 'localhost',
    'SERVER_PORT' = '6666',
    'SCHEDULER_MIN_TIME_BETWEEN_2_CALLS' = '1',
    'FINAUD_FREQUENCY' = '300',
    'MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING' = '25',
    'DETACH_JOB_FROM_SERVER' = '0',
    'LOG_FILE' = '/var/log/oar.log'
}

config.setdefault_config(DEFAULT_CONFIG)

# retrieve umask and set new one
old_umask = os.umask(oct('022'))

# TODO
# my $oldfh = select(STDERR); $| = 1; select($oldfh);
# $oldfh = select(STDOUT); $| = 1; select($oldfh);


# Everything is run by oar user (The real uid of this process.)
os.environ['OARDO_UID'] = str(os.geteuid())

# TODO
# my $Redirect_STD_process = OAR::Modules::Judas::redirect_everything();

logger = get_logger("oar.kao", forward_stderr=True)
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
    finishTag = True


# To avoid zombie processes
signal.signal(signal.SIGCHLD, signal.SIG_IGN) # Surely useless, ignoring SIGCHLD is the common default setting

signal.signal(signal.SIGUSR1, handler)
signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)

meta_sched_command = config['META_SCHED_CMD']
m = re.match(r'^\/', meta_sched_command_conf)
if not m:
    meta_sched_command = binpath + meta_sched_command
check_for_villains_command = binpath + 'sarko'
check_for_node_changes = binpath + 'finaud'
leon_command = binpath + 'leon'
nodeChangeState_command = binpath + 'NodeChangeState'
bipbip_command = binpath + 'bipbip'

server = None
server_port = config['SERVER_PORT']

servermaxconnect = 100

# This timeout is used by appendice to prevent a client to block
# reception by letting a connection opened
# should be left at a positive value
appendice_connection_timeout = 5

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
max_successive_read = 100

# Max waiting time before new scheduling attempt (in the case of
# no notification)
schedulertimeout = 60
# Min waiting time before 2 scheduling attempts
scheduler_min_time_between_2_calls = config['SCHEDULER_MIN_TIME_BETWEEN_2_CALLS']
scheduler_wanted = 0 # 1 if the scheduler must be run next time update

# Max waiting time before check for jobs whose time allowed has elapsed
villainstimeout = 10

# Max waiting time before check node states
checknodestimeout = config['FINAUD_FREQUENCY']

# Max number of concurrent bipbip processes
Max_bipbip_processes = config['MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING']
Detach_oarexec = config['DETACH_JOB_FROM_SERVER']

# Maximum duration a a bipbip process (after that time the process is killed)
Max_bipbip_process_duration = 30*60

Log_file = config['LOG_FILE'], "/var/log/oar.log")

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

# Internal stuff, not relevant for average user
lastscheduler = None
lastvillains = None
lastchecknodes = None
internal_command_file = []
appendice_pid = None

energy_pid = 0

# launch the command line passed in parameter
def launch_command(command):
    logger.debug('Launching command : [' + command + ']')
    # $ENV{PATH}="/bin:/usr/bin:/usr/local/bin";
    ####### THE LINE BELOW SHOULD NOT BE COMMENTED IN NORMAL USE ##### ??? TODO (BELOW -> ABOVE ???)
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    # system $command; ??? TODO  to remove ?

    pid = os.fork()
    if pid == 0:
         #CHILD
         signal.signal(signal.SIGUSR1, signal.SIG_IGN)
         signal.signal(signal.SIGINT, signal.SIG_IGN)
         signal.signal(signal.SIGTERM, signal.SIG_IGN)

    #
    # TODO USE subprocess ??? TOFINISH
    #

# listening procedure used by the appendice, a forked process dedicated
# to the listening of commands
def qget_appendice():
    # TODO to finish
    pass

# main body of the appendice, a forked process dedicated
# to the listening of commands
# the interest of such a forked process is to ensure that clients get their
# notification as soon as possible (i.e. reactivity) even if the almighty is
# performing some other internal task in the meantime
def comportement_appendice(): # TODO
    pass

# hulot module forking
def start_hulot(): # TODO
    pass

# check the hulot process
def check_hulot():
  return os.kill(energy_pid, 0)

# Clean ipcs
def ipc_clean(): # TODO
    pass

# initial stuff that has to be done
def init(): # TODO
    pass

# function used by the main automaton to get notifications pending
# inside the appendice
def qget(timeout): # TODO
    pass

# functions for managing the file of commands pending
def add_command(command):
    # as commands are just notifications that will
    # handle all the modifications in the base up to now, we should
    # avoid duplication in the command file
    m = re.compile('^' + command + '$')
    flag = True
    for cmd in internal_command_file:
        if re.match(m, cmd):
            flag = False
            break

    if flag:
        internal_command_file += command


# read commands until reaching the maximal successive read value or
# having read all of the pending commands
def read_commands(timeout): # TODO
    pass


# functions associated with each state of the automaton
def scheduler():
    return launch_command(scheduler_command)


def time_update:
    current = time.time()  # ---> TODO my $current = time; -> ???

    logger.debug('Timeouts check : ' + str(current))
    # check timeout for scheduler
    if (current >= (lastscheduler + schedulertimeout))\
       or (scheduler_wanted >= 1)\
       and (current >= (lastscheduler + scheduler_min_time_between_2_calls)):
        logger.debug('Scheduling timeout')
        # lastscheduler = current + schedulertimeout
        add_command('Scheduling')

    if current >= (lastvillains + villainstimeout):
        logger.debug("[Almighty] Villains check timeout\n");
        # lastvillains =  current +  villainstimeout
        add_command('Villains')

    if (current >= (lastchecknodes + checknodestimeout)) and (checknodestimeout > 0):
        logger.debug('Node check timeout')
        # lastchecknodes = -current + checknodestimeout
        add_command('Finaud')


def check_for_villains():
    return launch_command(check_for_villains_command)


def check_nodes():
    return launch_command(check_for_node_changes)


def leon():
    return launch_command(leon_command)


def nodeChangeState():
    return launch_command(nodeChangeState_command)



def init():


def main():
    state = 'Init'

    while True:
        logger.debug("Current state [" + state + "]")
        # We stop Almighty and its child
        if finishTag:
            if energy_pid:
                logger.debug("kill child process " + str(energy_pid))
                os.kill(energy_pid, signal.SIGKILL)
            logger.debug("kill child process " + str(appendice_pid))
            os.kill(appendice_pid, signal.SIGKILL)
            if Redirect_STD_process:
                os.kill(Redirect_STD_process, signal.SIGKILL)
            ipc_clean()
            logger.warning("Stop Almighty\n")
            send_log_by_email("Stop OAR server", "[Almighty] Stop Almighty")
            exit(10)

        # We check Hulot
        if energy_pid and not check_hulot():
            logger.warning("Energy saving module (hulot) died. Restarting it.")
            time.sleep(5)
            ipc_clean()
            start_hulot()

        # INIT
        if state == 'Init':
            init()
            state = 'Qget'

        # QGET
        elif state == 'Qget':
            if internal_command_file != []:
                read_commands(0)
            else:
                read_commands(read_commands_timeout)

        logger.debug('Command queue : ' + str(internal_command_file))
        current_command = internal_command_file.pop(0)
        command, arg1, arg2, arg3 = re.split(' ',current_command)

        logger.debug('Qtype = [' + command + ']')
        if (command == 'Qsub') or (command == 'Term') or (command == 'BipBip')\
           or (command == 'Scheduling') or (command == 'Qresume'):
            state = 'Scheduler'
        elif command == 'Qdel':
            state == 'Leon'
        elif command == 'Villains':
            state = 'Check for villains'
        elif command == 'Finaud':
            state = 'Check node states'
        elif command == 'Time':
            state = 'Time update'
        elif command == 'ChState':
            state = 'Change node state'
        else:
            logger.debug('Unknown command found in queue : ' + command)


        # SCHEDULER
        elif state == 'Scheduler':
            current_time = time.time()
            if current_time >= (lastscheduler + scheduler_min_time_between_2_calls):
                scheduler_wanted = 0
                # First, check pending events
                check_result = nodeChangeState()
                if check_result == 2:
                    state = 'Leon'
                    add_command('Term')
                elif check_result == 1:
                    state = 'Scheduler'
                elif check_result == 0:
                    # Launch the scheduler
                    # We check Hulot just before starting the scheduler
                    # because if the pipe is not read, it may freeze oar
                    if (energy_pid > 0) and not check_hulot():
                        logger.warning('Energy saving module (hulot) died. Restarting it.\n')
                        sleep 5
                        ipc_clean()
                        start_hulot()

                    scheduler_result = scheduler()
                    lastscheduler = time.time()
                    if scheduler_result == 0:
                        state = 'Time update'
                    elif scheduler_result == 1:
                        state = 'Scheduler'
                    elif scheduler_result == 2:
                        state = 'Leon'
                    else:
                        logger.error('Scheduler returned an unknown value : scheduler_result\n')
                        finishTag = 1

                else:
                    logger.error('nodeChangeState_command returned an unknown value\n')
                    finishTag = 1
            else:
                scheduler_wanted = 1
                state = 'Time update'
                logger.debug('Scheduler call too early, waiting... (' + current_time +\
                             '>= (' + lastscheduler + ' + ' + scheduler_min_time_between_2_calls + ')')

        # TIME UPDATE
        elif state == 'Time update':
            time_update()
            state = 'Qget'


        # CHECK FOR VILLAINS
        elif state == 'Check for villains':
            check_result = check_for_villains()
            lastvillains = time.time()
            if check_result == 1:
                state = 'Leon'
            elif check_result == 0:
                state = 'Time update'
            else:
                logger.error('check_for_villains_command returned an unknown value : check_result\n')
                finishTag = 1

        # CHECK NODE STATES
        elif state == 'Check node states':
            check_result = check_nodes()
            lastchecknodes = time.time()
            if check_result == 1:
                state = 'Change node state'
            elif check_result == 0:
                state = 'Time update'
            else:
                logger.error('check_for_node_changes returned an unknown value\n')
                finishTag = 1

        # LEON
        elif state == 'Leon':
             check_result = leon()
             state = 'Time update'
             if check_result == 1:
                 add_command('Term')

        # Change state for dynamic nodes
        elif state == 'Change node state':
            check_result=nodeChangeState()
            if check_result == 2:
                state = 'Leon'
                add_command('Term')
            elif check_result == 1:
                state = 'Scheduler'
            elif check_result == 0:
                state = 'Time update'
            else:
                logger.error('nodeChangeState_command returned an unknown value\n')
                finishTag = 1
        else:
            logger.warning('Critical bug !!!!\n')
            logger.error('Almighty just falled into an unknown state !!!\n')
            finishTag = 1

if __name__ == '__main__':  # pragma: no cover
    logger = get_logger('oar.kao', forward_stderr=True)
    main()
