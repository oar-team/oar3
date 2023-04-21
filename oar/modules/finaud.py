#!/usr/bin/env python
# coding: utf-8
"""
Check Alive and Suspected nodes.
"""
import sys

import oar.lib.tools as tools
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from oar.lib.event import add_new_event_with_host
from oar.lib.node import (
    get_current_assigned_nodes,
    get_finaud_nodes,
    set_node_nextState,
    update_node_nextFinaudDecision,
)


_,_,log = init_oar()
logger = get_logger(log, "oar.modules.finaud", forward_stderr=True)
logger.debug("Start Finaud")


class Finaud(object):
    def __init__(self, config):
        self.config = config
        self.return_value = 0

    def run(self, session):
        config = self.config
        logger.debug("Check Alive and Suspected nodes")
        if config["DB_TYPE"] != "Pg":
            logger.warning(
                "Distinct SQL part usage in get_finaud_nodes is not for sure well supported with SQLITE"
            )
        node_list_tmp = get_finaud_nodes(session)
        occupied_nodes = []
        check_occupied_nodes = "NO"

        if "CHECK_NODES_WITH_RUNNING_JOB" in config:
            check_occupied_nodes = config["CHECK_NODES_WITH_RUNNING_JOB"]

        if check_occupied_nodes == "NO":
            occupied_nodes = get_current_assigned_nodes(session)

        nodes_to_check = {}
        for node in node_list_tmp:
            if check_occupied_nodes == "NO":
                if node.network_address not in occupied_nodes:
                    nodes_to_check[node.network_address] = node
            else:
                nodes_to_check[node.network_address] = node

        logger.debug("Testing resource(s) on : " + ",".join(nodes_to_check.keys()))

        # Call the right program to test each nodes
        (pingcheck, bad_nodes) = tools.pingchecker(nodes_to_check.keys())
        if not pingcheck:
            bad_nodes = nodes_to_check.keys()
            logger.error("PingChecker timeouted")

        # Make the decisions
        for node in nodes_to_check.values():
            if (node.network_address in bad_nodes) and (node.state == "Alive"):
                set_node_nextState(session, node.network_address, "Suspected")
                update_node_nextFinaudDecision(session, node.network_address, "YES")
                add_new_event_with_host(session,
                    "FINAUD_ERROR",
                    0,
                    "Finaud has detected an error on the node",
                    [node.network_address],
                )
                self.return_value = 1
                logger.debug(
                    "Set the next state of " + node.network_address + " to Suspected"
                )

            elif (node.network_address not in bad_nodes) and (
                node.state == "Suspected"
            ):
                set_node_nextState(session, node.network_address, "Alive")
                update_node_nextFinaudDecision(session, node.network_address, "YES")
                add_new_event_with_host(session,
                    "FINAUD_RECOVER",
                    0,
                    "Finaud has detected that the node comes back",
                    [node.network_address],
                )
                self.return_value = 1
                logger.debug(
                    "Set the next state of " + node.network_address + " to Alive"
                )

        logger.debug("Finaud ended :" + str(self.return_value))


def main():  # pragma: no cover
    finaud = Finaud()
    finaud.run()
    return finaud.return_value


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
