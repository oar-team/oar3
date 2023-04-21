#!/usr/bin/env python
# coding: utf-8
"""
This module is responsible of the advanced management of the standby mode of the
nodes. It's related to the energy saving features of OAR. It is an optional module
activated with the ENERGY_SAVING_INTERNAL=yes configuration variable.

It runs as a fourth :mod:`oar.modules.almigthy` daemon and opens a pipe on which it receives commands
from the MetaScheduler. It also communicates with a library called "WindowForker"
that is responsible of forking shut-down/wake-up commands in a way that not too much
commands are started at a time.

-----------------------------------------------------------------------------------------------------

This module is responsible of waking up / shutting down nodes
when the scheduler decides it (writes it on a named pipe)

`CHECK` command is sent on the zmq PULL socket to :mod:`oar.modules.hulot` from different modules:

- By :mod:`oar.kao.meta_sched` if there is no node to wake up / shut down in order.
    - to check timeout and check memorized nodes list <TODO>
    - to check booting nodes status
- TOFINISH: Hulot will integrate window guarded launching processes
- By windowForker module:
    - to avoid zombie process
    - to messages received in queue (IPC)

Example of received message:

.. code-block:: JSON

    {
        "cmd": "WAKEUP",
        "nodes": ["node1", "node2" ]
    }
"""

import os
import os.path
import pickle
import re
import socket
import sys
from multiprocessing import Pool, TimeoutError

import zmq

import oar.lib.tools as tools
from oar.lib.database import wait_db_ready
from oar.lib.event import add_new_event_with_host
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from oar.lib.node import (
    change_node_state,
    get_alive_nodes_with_jobs,
    get_nodes_that_can_be_waked_up,
    get_nodes_with_given_sql,
)

_, _, logger = init_oar()
logger = get_logger(logger, "oar.modules.hulot", forward_stderr=True)


def check_reminded_list(
    nodes_list_running, nodes_list_to_remind, nodes_list_to_process
):
    # Checks if some nodes in list_to_remind can be processed
    nodes_toRemove = []
    for node, cmd_info in nodes_list_to_remind.items():
        if node not in nodes_list_running:
            # move this node from reminded list to list to process
            logger.debug(
                "Adding '" + node + "=>" + str(cmd_info) + "' to list to process."
            )
            nodes_list_to_process[node] = {
                "command": cmd_info["command"],
                "timeout": -1,
            }
    for node in nodes_toRemove:
        del nodes_list_to_remind[node]


# Fill the timeouts hash with the different timeouts
def fill_timeouts(str_timeouts):
    """
    Timeout to consider a node broken (suspected) if it has not woken up
    The value can be an integer of seconds or a set of pairs.
    For example, "1:500 11:1000 21:2000" will produce a timeout of 500
    seconds if 1 to 10 nodes have to wakeup, 1000 seconds if 11 t 20 nodes
    have to wake up and 2000 seconds otherwise.
    ENERGY_SAVING_NODE_MANAGER_WAKEUP_TIMEOUT="900"
    """

    timeouts = {}
    if isinstance(str_timeouts, int):
        timeouts[1] = str_timeouts
    elif re.match(r"^\s*\d+\s*$", str_timeouts):
        timeouts[1] = int(str_timeouts)
    else:
        # Remove front and final spaces
        str_timeouts = re.sub(r"^\s+|\s+$", "", str_timeouts)
        for str_nb_timeout in re.split(r"\s+", str_timeouts):
            # Each couple of values is only composed of digits separated by colon
            if re.match(r"^\d+:\d+$", str_nb_timeout):
                nb_timeout = re.split(r":", str_nb_timeout)
                timeouts[int(nb_timeout[0])] = int(nb_timeout[1])
            else:
                logger.warning(nb_timeout + " is not a valid couple for a timeout")
    if not timeouts:
        timeouts[1] = 900
        logger.warning(
            "Timeout not properly defined, using default value: " + str(timeouts[1])
        )

    return timeouts


# Choose a timeout based on the number of nodes to wake up
def get_timeout(timeouts, nb_nodes):
    timeout = timeouts[1]
    # Search for the timeout of the corresponding interval
    for nb in sorted(timeouts.keys()):
        if nb_nodes < nb:
            break
        timeout = timeouts[nb]
    return timeout


class HulotClient(object):
    """Hulot client part used by metascheduler to interact with Hulot server"""

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        # Initialize zeromq context
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.setsockopt(
            zmq.LINGER, 5000
        )  # To allow client program exit if Hulot is not ready
        try:
            self.socket.connect(
                "tcp://" + config["HULOT_SERVER"] + ":" + str(config["HULOT_PORT"])
            )
        except Exception as e:
            logger.error(f"Failed to connect to Hulot: {e}")
            exit(1)

    def check_nodes(self):
        self.socket.send_json({"cmd": "CHECK"})

    def halt_nodes(self, nodes):
        self.socket.send_json({"cmd": "HALT", "nodes": nodes})

    def wake_up_nodes(self, nodes):
        self.socket.send_json({"cmd": "WAKEUP", "nodes": nodes})


class Hulot(object):
    def __init__(self, config, logger):
        logger.info("Initiating Hulot, the energy saving module")
        self.logger = logger
        self.config = config

        self.exit_code = 0
        # Intialize zeromq context
        self.context = zmq.Context()
        # IP addr is required when bind function is used on zmq socket
        ip_addr_hulot = socket.gethostbyname(config["HULOT_SERVER"])
        self.socket = self.context.socket(zmq.PULL)
        try:
            self.socket.bind("tcp://" + ip_addr_hulot + ":" + str(config["HULOT_PORT"]))
        except Exception as e:
            logger.error(f"Failed to bind Hulot endpoint: {e}")
            exit(1)

        # self.executors_socket = self.context.socket(zmq.PULL)
        # try:
        #    self.socket.bind('ipc://tmp/oar_executor_notification')
        # except:
        #    logger.error('Failed to bind Hulot endpoint to receive executor notifications')
        #    exit(1)

        # self.executors_socket.RCVTIMEO = 0 # Set to non-blocking socket

        self.timeouts = fill_timeouts(
            config["ENERGY_SAVING_NODE_MANAGER_WAKEUP_TIMEOUT"]
        )

        self.executors = []
        # if (config['ENERGY_SAVING_WINDOW_FORKER_BYPASS'] == 'no']:
        #    self.max_executors = -1
        # else:
        self.max_executors = int(config["ENERGY_SAVING_WINDOW_FORKER_SIZE"])

        # Load state if exists
        self.nodes_list_running = {}
        self.nodes_list_to_remind = {}
        self.hulot_status_dump_name = (
            config["OAR_RUNTIME_DIRECTORY"] + "hulot_status.dump"
        )
        if os.path.isfile(self.hulot_status_dump_name):
            with open(self.hulot_status_dump_name, "rb") as f:
                hulot_status_dump = pickle.load(f)
                self.nodes_list_running = hulot_status_dump["nodes_list_running"]
                self.nodes_list_to_remind = hulot_status_dump["nodes_list_to_remind"]

                # with open('obj/'+ name + '.pkl', 'wb') as f:
                #
            os.remove(self.hulot_status_dump_name)

        # Init keepalive values ie construct a hash:
        #      sql properties => number of nodes to keepalive
        # given the ENERGY_SAVING_NODES_KEEPALIVE variable such as:
        # "cluster=paradent:nodes=4,cluster=paraquad:nodes=6"

        # Number of nodes to keepalive per properties:
        #     $keepalive{<properties>}{"min"}=int
        # Number of nodes currently alive and with no jobs, per properties:
        #     $keepalive{<properties>}{"cur_idle"}=int
        # List of nodes corresponding to properties:
        #     $keepalive{<properties>}{"nodes"}=@;

        self.keepalive = {}
        str_keepalive = config["ENERGY_SAVING_NODES_KEEPALIVE"]
        if not re.match(r".+:\d+,*", str_keepalive):
            logger.error("Syntax error into ENERGY_SAVING_NODES_KEEPALIVE !")
            self.exit_code = 3
            return
        else:
            for keepalive_item in str_keepalive.split(","):
                prop_nb = keepalive_item.split(":")
                properties = prop_nb[0]
                nb_nodes = prop_nb[1]
                if not re.match(r"^(\d+)$", nb_nodes):
                    logger.error(
                        "Syntax error into ENERGY_SAVING_NODES_KEEPALIVE ! (not an integer)"
                    )
                    self.exit_code = 2
                    return
                self.keepalive[properties] = {"nodes": [], "min": int(nb_nodes)}
                logger.debug("Keepalive(" + properties + ") => " + nb_nodes)
        self.window_forker = WindowForker(
            config["ENERGY_SAVING_WINDOW_FORKER_SIZE"],
            config["ENERGY_SAVING_WINDOW_TIMEOUT"],
            config,
        )
        # TODO
        # my $count_cycles;
        #

    def run(self, session, loop=True):
        logger = self.logger
        config = self.config

        logger.info("Starting Hulot's main loop")
        nodes_list_to_process = {}
        nodes_list_to_remind = self.nodes_list_to_remind
        nodes_list_running = self.nodes_list_running
        keepalive = self.keepalive
        count_cycles = 1

        # wait db at launch
        try:
            wait_db_ready(get_alive_nodes_with_jobs, args=[session])
        except Exception as e:
            logger.error(f"Failed to contact database: {e}")
            return 1

        while True:
            self.window_forker.check_executors(session, config, nodes_list_running)

            message = self.socket.recv_json()

            command = message["cmd"]
            nodes = []
            if "nodes" in message:
                nodes = message["nodes"]

            if command == "CHECK":
                logger.debug("Got request: " + command)
            else:
                logger.debug("Got request: " + command + " for nodes: " + str(nodes))

            # Identify idle and occupied nodes
            all_occupied_nodes = get_alive_nodes_with_jobs(session)
            nodes_that_can_be_waked_up = get_nodes_that_can_be_waked_up(
                session, tools.get_date(session)
            )

            for properties in keepalive.keys():
                occupied_nodes = []
                idle_nodes = []
                keepalive[properties]["nodes"] = [
                    p for p in get_nodes_with_given_sql(session, properties)
                ]
                keepalive[properties]["current_idle"] = 0
                alive_nodes = get_nodes_with_given_sql(
                    session, properties + "and (state='Alive' or next_state='Alive')"
                )
                for alive_node in alive_nodes:
                    if alive_node in all_occupied_nodes:
                        occupied_nodes.append(alive_node)
                    else:
                        keepalive[properties]["current_idle"] += 1
                        idle_nodes.append(alive_node)

                logger.debug(
                    "current_idle("
                    + properties
                    + ") => "
                    + str(keepalive[properties]["current_idle"])
                )

                # Wake up some nodes corresponding to properties if needed
                ok_nodes = (
                    keepalive[properties]["current_idle"] - keepalive[properties]["min"]
                )
                keep_nodes = keepalive[properties]["nodes"]
                # wakeable_nodes = keep_nodes - occupied_nodes - idle_nodes
                wakeable_nodes = [
                    n
                    for n in keep_nodes
                    if (n not in occupied_nodes) and (n not in idle_nodes)
                ]

                for node in wakeable_nodes:
                    if ok_nodes >= 0:
                        break
                    # we have a good candidate to wake up
                    # now, check if the node has a good status
                    if node in nodes_that_can_be_waked_up:
                        ok_nodes += 1
                        # add WAKEUP: node to list of commands if not already
                        # into the current command list
                        if node not in nodes_list_running:
                            nodes_list_to_process[node] = {
                                "command": "WAKEUP",
                                "timeout": -1,
                            }
                            logger.debug(
                                "Waking up "
                                + node
                                + " to satisfy '"
                                + properties
                                + "' keepalive (ok_nodes="
                                + str(ok_nodes)
                                + ", wakeable_nodes="
                                + str(len(wakeable_nodes))
                                + ")"
                            )
                        else:
                            if nodes_list_running[node]["command"] != "WAKEUP":
                                logger.debug(
                                    "Wanted to wake up "
                                    + node
                                    + " to satisfy '"
                                    + properties
                                    + "' keepalive, but a command is already running on this node. "
                                    + "So doing nothing and waiting for the next cycles to converge."
                                )

            # Retrieve list of nodes having at least one resource Alive
            nodes_alive = get_nodes_with_given_sql(session, "state='Alive'")

            # Checks if some booting nodes need to be suspected
            nodes_toRemove = []
            for node, cmd_info in nodes_list_running.items():
                if cmd_info["command"] == "WAKEUP":
                    if node in nodes_alive:
                        logger.debug(
                            "Booting node '"
                            + node
                            + "' seems now up, so removing it from running list."
                        )
                        # Remove node from the list running nodes
                        nodes_toRemove.append(node)
                    elif tools.get_date(session) > cmd_info["timeout"]:
                        change_node_state(session, node, "Suspected", config)
                        info = (
                            "Node "
                            + node
                            + "was suspected because it did not wake up before the end of the timeout"
                        )
                        add_new_event_with_host(
                            session, "LOG_SUSPECTED", 0, info, [node]
                        )
                        # Remove suspected node from the list running nodes
                        nodes_toRemove.append(node)
                        # Remove this node from received list (if node is present) because it was suspected
                        if node in nodes:
                            nodes.remove(node)

            for node in nodes_toRemove:
                del nodes_list_running[node]

            # Check if some nodes in list_to_remind can be processed
            check_reminded_list(
                nodes_list_running, nodes_list_to_remind, nodes_list_to_process
            )

            # Checking if each couple node/command was already received or not
            for node in nodes:
                node_finded = False
                node_toAdd = False
                node_toRemind = False
                if nodes_list_running:
                    # Checking

                    for node_running, cmd_info in nodes_list_running.items():
                        if node == node_running:
                            node_finded = True
                            if command != cmd_info["command"]:
                                # This node is already planned for an other action
                                # We have to keep in memory this new couple node/command
                                node_toRemind = True
                            else:
                                logger.debug(
                                    "Command '"
                                    + cmd_info["command"]
                                    + "' is already running on node '"
                                    + node
                                    + "'"
                                )

                    if not node_finded:
                        node_toAdd = True

                else:
                    node_toAdd = True

                if node_toAdd:
                    # Adding couple node/command to the list to process
                    logger.debug(
                        "Adding '" + node + "=>" + command + "' to list to process"
                    )
                    nodes_list_to_process[node] = {"command": command, "timeout": -1}

                if node_toRemind:
                    # Adding couple node/command to the list to remind
                    logger.debug(
                        "Adding '" + node + "=>" + command + "' to list to remember"
                    )
                    nodes_list_to_remind[node] = {"command": command, "timeout": -1}

            # Creating command list
            command_toLaunch = []
            match = False
            # Get the timeout taking into account the number of nodes
            # already waking up + the number of nodes to wake up
            timeout = get_timeout(
                self.timeouts, len(nodes_list_running) + len(nodes_list_to_process)
            )

            nodes_toRemove_from_list_to_process = []
            for node, cmd_info in nodes_list_to_process.items():
                cmd = cmd_info["command"]
                if cmd == "WAKEUP":
                    # Save the timeout for the nodes to be processed.
                    cmd_info["timeout"] = tools.get_date(session) + timeout
                    command_toLaunch.append(("WAKEUP", node))
                elif cmd == "HALT":
                    # Don't halt nodes that needs to be kept alive
                    match = False
                    for properties, prop_info in keepalive.items():
                        nodes_keepalive = prop_info["nodes"]
                        if node in nodes_keepalive:
                            if prop_info["current_idle"] <= prop_info["min"]:
                                logger.debug(
                                    "Not halting '"
                                    + node
                                    + "' because I need to keep alive "
                                    + str(prop_info["min"])
                                    + " nodes having '"
                                    + properties
                                    + "'"
                                )
                                match = True

                                if node in nodes_list_running:
                                    del nodes_list_running[node]
                                nodes_toRemove_from_list_to_process.append(node)
                    # If the node is ok to be halted
                    if not match:
                        # Update the keepalive counts
                        for properties, prop_info in keepalive.items():
                            nodes = prop_info["nodes"]
                            if node in nodes:
                                prop_info["current_idle"] -= 1

                        # Change state node to "Absent" and halt it
                        change_node_state(session, node, "Absent", config)
                        logger.debug(
                            "Hulot module puts node '"
                            + node
                            + "' in energy saving mode (state: Absent/StandBy)"
                        )
                        command_toLaunch.append(("HALT", node))

                else:
                    logger.error(
                        "Unknown command: '" + cmd + "' for node '" + node + "'"
                    )
                    return 1

            # Remove nodes to process list if needed
            # (disable HALT cmd to satisfy keepAlive condition)

            for node in nodes_toRemove_from_list_to_process:
                del nodes_list_to_process[node]

            # Launching commands
            if command_toLaunch:
                logger.debug("Launching commands to nodes")
                self.window_forker.add_commands_toLaunch(session, command_toLaunch)

            # Adds to running list last new launched commands
            for node, cmd_info in nodes_list_to_process.items():
                nodes_list_running[node] = cmd_info

            # Cleaning the list to process
            nodes_list_to_process = {}

            # From Hulot.pm
            # Suicide to workaround eventaul memory leaks. Almighty will restart hulot.
            # TODO ? do we need it ?
            count_cycles += 1

            if count_cycles >= config["ENERGY_MAX_CYCLES_UNTIL_REFRESH"]:
                # Save state
                with open(self.hulot_status_dump_name, "wb") as dump_file:
                    hulot_status_dump_name = {
                        "nodes_list_running": nodes_list_running,
                        "nodes_list_to_remind": nodes_list_to_remind,
                    }
                    pickle.dump(
                        hulot_status_dump_name, dump_file, pickle.HIGHEST_PROTOCOL
                    )
                return 42

            if not loop:
                break
        return 0


def command_executor(cmd_node, config):
    command, node = cmd_node
    command_to_exec = 'echo "' + node + '" | '
    if command == "HALT":
        if "ENERGY_SAVING_NODE_MANAGER_SLEEP_CMD" not in config:
            logger.error("ENERGY_SAVING_NODE_MANAGER_SLEEP_CMD is undefined")
        command_to_exec += config["ENERGY_SAVING_NODE_MANAGER_SLEEP_CMD"]
    else:
        if "ENERGY_SAVING_NODE_MANAGER_WAKE_UP_CMD" not in config:
            logger.error("ENERGY_SAVING_NODE_MANAGER_WAKE_UP_CMD is undefined")
        command_to_exec += config["ENERGY_SAVING_NODE_MANAGER_WAKE_UP_CMD"]

    exit_code = tools.call(command_to_exec, shell=True)
    return exit_code


class WindowForker(object):
    def __init__(self, window_size, timeout, config):
        self.config = config
        self.timeout = timeout
        self.pool = Pool(processes=window_size)
        self.executors = {}

    def add_commands_toLaunch(self, session, commands):
        # Build strings to pass to wakeup and shutdown commands
        halt_nodes = []
        wakeup_nodes = []
        for cmd_node in commands:
            cmd, node = cmd_node
            if cmd == "HALT":
                halt_nodes.append(node)
            else:  # cmd == 'WAKEUP'
                wakeup_nodes.append(node)

        if halt_nodes:
            add_new_event_with_host(
                session, "HALT_NODE", 0, "Node " + node + " halt request", halt_nodes
            )
        if wakeup_nodes:
            add_new_event_with_host(
                session,
                "WAKEUP_NODE",
                0,
                "Node " + node + " wake-up request",
                wakeup_nodes,
            )

        for cmd_node in commands:
            cmd, node = cmd_node
            # FIXME: Async code here ?!
            self.executors[
                self.pool.apply_async(command_executor, (cmd_node, self.config))
            ] = (
                node,
                cmd,
                tools.get_date(session),
            )

    def check_executors(self, session, config, nodes_list_running):
        executors_toRemove = []
        now = tools.get_date(
            session,
        )
        for executor, data in self.executors.items():
            node, cmd, launching_date = data
            if executor.ready():  # TODO executor.successful()
                executors_toRemove.append(executor)
                exit_status = executor.get()
                if exit_status != 0:
                    # Suspect node if error
                    change_node_state(session, node, "Suspected", config)
                    message = (
                        "Node "
                        + node
                        + " was suspected because an error occurred with a command launched by Hulot"
                    )
                    add_new_event_with_host(
                        session, "LOG_SUSPECTED", 0, message, [node]
                    )
                else:
                    if cmd == "HALT":  # WAKEUP case is addressed in main run loop
                        del nodes_list_running[node]

            elif now - launching_date > self.timeout:
                executors_toRemove.append(executor)
                try:
                    # Force timeout to finish executor
                    executor.get(timeout=0)
                except TimeoutError:
                    if cmd == "HALT":  # WAKEUP case is addressed in main run loop
                        # Suspect node if error
                        change_node_state(session, node, "Suspected", config)
                        message = (
                            "Node "
                            + node
                            + " was suspected because shutdown command launched by Hulot timeouted"
                        )
                        add_new_event_with_host(
                            session, "LOG_SUSPECTED", 0, message, [node]
                        )
                        del nodes_list_running[node]

        for executor in executors_toRemove:
            del self.executors[executor]


def main():  # pragma: no cover
    hulot = Hulot()
    if hulot.exit_code:
        return hulot.exit_code
    return hulot.run()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
