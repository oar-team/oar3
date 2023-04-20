#!/usr/bin/env python
# coding: utf-8
"""
This module is the OAR server. It decides what actions must be performed. It is divided into 3 processes:

    - One listens to a TCP/IP socket. It waits information or commands from OAR
      user program or from the other modules.
    - Another one deals with commands thanks to an automaton and launch right
      modules one after one.
    - The third one handles a pool of forked processes that are used to launch and
      stop the jobs.

"""
import os
import re
import signal
import socket
import sys
import time

import zmq

import oar.lib.tools as tools
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger

config, db, logger = init_oar()

# Set undefined config value to default one
DEFAULT_CONFIG = {
    "META_SCHED_CMD": "kao",
    "SERVER_HOSTNAME": "localhost",
    "APPENDICE_SERVER_PORT": "6670",  # new endpoint which replaces appendice
    "SCHEDULER_MIN_TIME_BETWEEN_2_CALLS": "1",
    "FINAUD_FREQUENCY": "300",
    "LOG_FILE": "/var/log/oar.log",
    "ENERGY_SAVING_INTERNAL": "no",
}

config.setdefault_config(DEFAULT_CONFIG)

# Everything is run by oar user (The real uid of this process.)
os.environ["OARDO_UID"] = str(os.geteuid())

logger = get_logger(logger, "oar.modules.almighty", forward_stderr=True)
logger.info("Start Almighty")
# TODO
# send_log_by_email("Start OAR server","[Almighty] Start Almighty");

if "OARDIR" in os.environ:
    binpath = os.environ["OARDIR"]
else:
    binpath = "/usr/local/lib/oar/"
    logger.warning(
        "OARDIR env variable must be defined, set it to default value:" + binpath
    )
    os.environ["OARDIR"] = binpath

meta_sched_command = config["META_SCHED_CMD"]
m = re.match(r"^\/", meta_sched_command)
if not m:
    meta_sched_command = os.path.join(binpath, meta_sched_command)

leon_command = os.path.join(binpath, "oar-leon")
check_for_villains_command = os.path.join(binpath, "oar-sarko")
check_for_node_changes = os.path.join(binpath, "oar-finaud")
nodeChangeState_command = os.path.join(binpath, "oar-node-change-state")

# Legacy OAR2
# leon_command = binpath + 'Leon'
# check_for_villains_command = binpath + 'sarko'
# check_for_node_changes = binpath + 'finaud'
# nodeChangeState_command = binpath + 'NodeChangeState'
# nodeChangeState_command = 'true'

proxy_appendice_command = os.path.join(binpath, "oar-appendice-proxy")
bipbip_commander = os.path.join(binpath, "oar-bipbip-commander")
hulot_command = os.path.join(binpath, "oar-hulot")

# This timeout is used to slowdown the main automaton when the
# command queue is empty, it correspond to a blocking read of
# new commands. A High value is likely to reduce the CPU usage of
# the Almighty.
# Setting it to 0 or a low value is not likely to improve performance
# dramatically (because it blocks only when nothing else is to be done).
# Nevertheless it is closely related to the precision at which the
# internal counters are checked
read_commands_timeout = 5 * 1000  # in ms

# This parameter sets the number of pending commands read from
# appendice before proceeding with internal work
# should not be set at a too high value as this would make the
# Almighty weak against flooding
max_successive_read = 1

# Max waiting time before new scheduling attempt (in the case of
# no notification)
schedulertimeout = 60
# Min waiting time before 2 scheduling attempts
scheduler_min_time_between_2_calls = int(config["SCHEDULER_MIN_TIME_BETWEEN_2_CALLS"])


# Max waiting time before check for jobs whose time allowed has elapsed
villainstimeout = 10

# Max waiting time before check node states
checknodestimeout = int(config["FINAUD_FREQUENCY"])

Log_file = config["LOG_FILE"]

energy_pid = 0

# Signal handle
finishTag = False


# The signal handler must take two arguments
# https://docs.python.org/3.8/library/signal.html#signal.signal
def signal_handler(sig, stack):
    global finishTag
    finishTag = True


#
# To avoid zombie processes
#


signal.signal(signal.SIGUSR1, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def launch_command(command):
    """Launch the command line passed in parameter"""

    # TODO move to oar.lib.tools
    # global finishTag

    logger.debug("Launching command : [" + command + "]")

    p = tools.Popen(command, stdout=tools.PIPE, stderr=tools.PIPE, shell=True)
    stdout, stderr = p.communicate()
    return_code = p.wait()

    logger.debug(command + " terminated")
    logger.debug("Exit value : " + str(return_code))

    if return_code != 0:
        logger.debug("Command failed with error: {}".format(stderr.decode("utf-8")))

    return return_code


def start_hulot():
    """Start :mod:`oar.kao.hulot`"""
    return tools.Popen(hulot_command)


def check_hulot(hulot):
    """Check the presence hulot process"""
    return tools.check_process(hulot.pid)


#
# functions associated with each state of the automaton
#


def meta_scheduler():
    """Start :mod:`oar.kao.meta_sched`"""
    return launch_command(meta_sched_command)


def check_for_villains():
    """Start :mod:`oar.modules.sarko`"""
    return launch_command(check_for_villains_command)


def check_nodes():
    """Start :mod:`oar.modules.finaud`"""
    return launch_command(check_for_node_changes)


def leon():
    """Start :mod:`oar.modules.leon`"""
    return launch_command(leon_command)


def nodeChangeState():
    """Start :mod:`oar.modules.node_change_state`"""
    return launch_command(nodeChangeState_command)


class Almighty(object):
    def __init__(self):
        self.state = "Init"
        logger.debug("Current state [" + self.state + "]")

        # Activate appendice socket
        self.context = zmq.Context()
        self.appendice = self.context.socket(zmq.PULL)
        ip_addr_server = socket.gethostbyname(config["SERVER_HOSTNAME"])
        try:
            self.appendice.bind(
                "tcp://" + ip_addr_server + ":" + config["APPENDICE_SERVER_PORT"]
            )
        except Exception as e:
            logger.error(f"Failed to activate appendice endpoint: {e}")
            sys.exit(1)

        self.set_appendice_timeout(read_commands_timeout)

        # Starting of Hulot, the Energy saving module
        self.hulot = None
        if config["ENERGY_SAVING_INTERNAL"] == "yes":
            self.hulot = start_hulot()

        self.lastscheduler = 0
        self.lastvillains = 0
        self.lastchecknodes = 0
        self.command_queue = []

        self.scheduler_wanted = 0  # 1 if the scheduler must be run next time update

        logger.debug("Init done")
        self.state = "Qget"

        self.start_companions()

    def start_companions(self):
        """Start appendice :mod:`oar.modules.appendice_proxy` and :mod:`oar.modules.bipbip_commander` commander processes"""

        self.appendice_proxy = tools.Popen(proxy_appendice_command)
        self.bipbip_commander = tools.Popen(bipbip_commander)

    def time_update(self):
        current = tools.get_time()  # ---> TODO my $current = time; -> ???

        logger.debug("Timeouts check : " + str(current))
        # check timeout for scheduler
        if (
            (current >= (self.lastscheduler + schedulertimeout))
            or (self.scheduler_wanted >= 1)
            and (current >= (self.lastscheduler + scheduler_min_time_between_2_calls))
        ):
            logger.debug("Scheduling timeout")
            # lastscheduler = current + schedulertimeout
            self.add_command("Scheduling")

        if current >= (self.lastvillains + villainstimeout):
            logger.debug("Villains check timeout")
            # lastvillains =  current +  villainstimeout
            self.add_command("Villains")

        if (current >= (self.lastchecknodes + checknodestimeout)) and (
            checknodestimeout > 0
        ):
            logger.debug("Node check timeout")
            # lastchecknodes = -current + checknodestimeout
            self.add_command("Finaud")

    def set_appendice_timeout(self, timeout):
        """Set timeout appendice socket"""
        self.appendice.RCVTIMEO = timeout

    def qget(self, timeout):
        """function used by the main automaton to get notifications from appendice"""

        # timeout = 10 * 1000
        self.set_appendice_timeout(timeout)

        logger.debug("Timeout value:" + str(timeout))

        try:
            answer = self.appendice.recv_json()
        except zmq.error.Again as e:
            logger.debug("Timeout from appendice:" + str(e))
            # return (None, {'cmd': 'Time'})
            return {"cmd": "Time"}
        except zmq.ZMQError as e:
            logger.error("Something is wrong with appendice" + str(e))
            # return (15, None)
            return {"cmd": "Time"}
        # return (None, answer)
        return answer

    def add_command(self, command):
        """as commands are just notifications that will
        handle all the modifications in the base up to now, we should
        avoid duplication in the command file"""

        m = re.compile("^" + command + "$")
        flag = True
        for cmd in self.command_queue:
            if re.match(m, cmd):
                flag = False
                break

        if flag:
            self.command_queue.append(command)

    def read_commands(self, timeout=read_commands_timeout):  # TODO
        """read commands until reaching the maximal successive read value or
        having read all of the pending commands"""

        command = None
        remaining = max_successive_read

        while (command != "Time") and remaining:
            command = self.qget(timeout)
            if remaining != max_successive_read:
                timeout = 0
            if command is None:
                break
            self.add_command(command["cmd"])
            remaining -= 1
            logger.debug(
                "Got command " + command["cmd"] + ", " + str(remaining) + " remaining"
            )

    def run(self, loop=True):
        """Start :mod:`oar.modules.almigthy` main loop."""
        global finishTag
        while True:
            logger.debug("Current state [" + self.state + "]")
            # We stop Almighty and its child
            if finishTag:
                if energy_pid:
                    logger.debug("kill child process " + str(energy_pid))
                    tools.kill(energy_pid, signal.SIGKILL)
                # TODO:  $Redirect_STD_process = OAR::Modules::Judas::redirect_everything();
                Redirect_STD_process = False
                if Redirect_STD_process:
                    tools.kill(Redirect_STD_process, signal.SIGKILL)
                # TODO ipc_clean()
                logger.warning("Stop Almighty\n")
                # TODO: send_log_by_email("Stop OAR server", "[Almighty] Stop Almighty")
                return 10

            # We check Hulot
            if self.hulot and not check_hulot(self.hulot):
                logger.warning("Energy saving module (hulot) died. Restarting it.")
                start_hulot(self)
            # QGET
            elif self.state == "Qget":
                # if len(self.command_queue) > 0:
                # self.read_commands(0)
                #    pass
                # else:
                self.read_commands(read_commands_timeout)

                logger.debug("Command queue : " + str(self.command_queue))
                command = self.command_queue.pop(0)
                # Remove useless 'Time' command to enhance reactivity
                if command == "Time" and self.command_queue != []:
                    command = self.command_queue.pop(0)

                logger.debug("Qtype = [" + command + "]")
                if (
                    (command == "Qsub")
                    or (command == "Qsub -I")
                    or (command == "Term")
                    or (command == "BipBip")
                    or (command == "Scheduling")
                    or (command == "Qresume")
                    or (command == "Walltime")
                ):
                    self.state = "Scheduler"
                elif command == "Qdel":
                    self.state = "Leon"
                elif command == "Villains":
                    self.state = "Check for villains"
                elif command == "Finaud":
                    self.state = "Check node states"
                elif command == "Time":
                    self.state = "Time update"
                elif command == "ChState":
                    self.state = "Change node state"
                else:
                    logger.error("Unknown command found in queue : " + command)

            # SCHEDULER
            elif self.state == "Scheduler":
                current_time = tools.get_time()
                if current_time >= (
                    self.lastscheduler + scheduler_min_time_between_2_calls
                ):
                    self.scheduler_wanted = 0
                    # First, check pending events
                    check_result = nodeChangeState()
                    if check_result == 2:
                        self.state = "Leon"
                        self.add_command("Term")
                    elif check_result == 1:
                        self.state = "Scheduler"
                    elif check_result == 0:
                        # Launch the scheduler
                        # We check Hulot just before starting the scheduler
                        # because if the pipe is not read, it may freeze oar
                        if (energy_pid > 0) and not check_hulot():
                            logger.warning(
                                "Energy saving module (hulot) died. Restarting it."
                            )
                            time.sleep(5)
                            start_hulot()

                        scheduler_result = meta_scheduler()
                        self.lastscheduler = tools.get_time()
                        if scheduler_result == 0:
                            self.state = "Time update"
                        elif scheduler_result == 1:
                            self.state = "Scheduler"
                        elif scheduler_result == 2:
                            self.state = "Leon"
                        else:
                            logger.error(
                                "Scheduler returned an unknown value : scheduler_result"
                            )
                            finishTag = 1

                    else:
                        logger.error(
                            "nodeChangeState_command returned an unknown value."
                        )
                        finishTag = 1
                else:
                    self.scheduler_wanted = 1
                    self.state = "Time update"
                    logger.debug(
                        "Scheduler call too early, waiting... ("
                        + str(current_time)
                        + ">= ("
                        + str(self.lastscheduler)
                        + " + "
                        + str(scheduler_min_time_between_2_calls)
                        + ")"
                    )

            # TIME UPDATE
            elif self.state == "Time update":
                self.time_update()
                self.state = "Qget"

            # CHECK FOR VILLAINS
            elif self.state == "Check for villains":
                check_result = check_for_villains()
                self.lastvillains = tools.get_time()
                if check_result == 1:
                    self.state = "Leon"
                elif check_result == 0:
                    self.state = "Time update"
                else:
                    logger.error(
                        "check_for_villains_command returned an unknown value : check_result."
                    )
                    finishTag = 1

            # CHECK NODE STATES
            elif self.state == "Check node states":
                check_result = check_nodes()
                self.lastchecknodes = tools.get_time()
                if check_result == 1:
                    self.state = "Change node state"
                elif check_result == 0:
                    self.state = "Time update"
                else:
                    logger.error("check_for_node_changes returned an unknown value.")
                    finishTag = 1

            # LEON
            elif self.state == "Leon":
                check_result = leon()
                self.state = "Time update"
                if check_result == 1:
                    self.add_command("Term")

            # Change state for dynamic nodes
            elif self.state == "Change node state":
                check_result = nodeChangeState()
                if check_result == 2:
                    self.state = "Leon"
                    self.add_command("Term")
                elif check_result == 1:
                    self.state = "Scheduler"
                elif check_result == 0:
                    self.state = "Time update"
                else:
                    logger.error("nodeChangeState_command returned an unknown value.")
                    finishTag = 1
            else:
                logger.warning("Critical bug !!!!\n")
                logger.error("Almighty just falled into an unknown state !!!.")
                finishTag = 1

            if not loop:
                break
        return 0


def main():  # pragma: no cover
    almighty = Almighty()
    return almighty.run()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
