#!/usr/bin/env python
import logging
import os
import subprocess
import time
from typing import Any, Dict, List, Optional

import yaml

from oar.lib.database import wait_db_ready
from oar.lib.globals import init_and_get_session, init_config
from oar.lib.node import get_alive_nodes_with_jobs, get_nodes_with_given_sql

# Get config variables and sets defaults if not defined into the oar.conf file

config = init_config()

PHOENIX_STATUS_FILE: str
PHOENIX_LOGDIR: str
PHOENIX_SOFT_REBOOTCMD: str
PHOENIX_SOFT_TIMEOUT: int
PHOENIX_HARD_REBOOTCMD: str
PHOENIX_HARD_TIMEOUT: int
PHOENIX_MAX_REBOOTS: int
PHOENIX_CMD_TIMEOUT: int
PHOENIX_BROKEN_NODES: str

# File where phoenix saves it's state
if "PHOENIX_STATUS_FILE" not in config:
    PHOENIX_STATUS_FILE = "/var/lib/oar/phoenix/oar_phoenix.state"
else:
    PHOENIX_STATUS_FILE = config["PHOENIX_DBFILE"]

# Directory where logfiles are created in case of problems
if "PHOENIX_LOGDIR" not in config:
    PHOENIX_LOGDIR = "/var/lib/oar/phoenix/"
else:
    PHOENIX_LOGDIR = config["PHOENIX_LOGDIR"]

# Command sent to reboot a node (first attempt)
if "PHOENIX_SOFT_REBOOTCMD" not in config:
    PHOENIX_SOFT_REBOOTCMD = "echo 'Soft reboot command for {NODENAME}: PHOENIX_SOFT_REBOOTCMD not configured'"
else:
    PHOENIX_SOFT_REBOOTCMD = config["PHOENIX_SOFT_REBOOTCMD"]

# Timeout for a soft rebooted node to be considered hard rebootable
if "PHOENIX_SOFT_TIMEOUT" not in config:
    PHOENIX_SOFT_TIMEOUT = 300
else:
    PHOENIX_SOFT_TIMEOUT = int(config["PHOENIX_SOFT_TIMEOUT"])

# Command sent to reboot a node (second attempt)
if "PHOENIX_HARD_REBOOTCMD" not in config:
    PHOENIX_HARD_REBOOTCMD = "echo 'Hard reboot command for {NODENAME}: PHOENIX_HARD_REBOOTCMD not configured'"
else:
    PHOENIX_HARD_REBOOTCMD = config["PHOENIX_HARD_REBOOTCMD"]

# Timeout (s) for a hard rebooted node to be considered really broken, then an email is sent (TODO)
if "PHOENIX_HARD_TIMEOUT" not in config:
    PHOENIX_HARD_TIMEOUT = 300
else:
    PHOENIX_HARD_TIMEOUT = int(config["PHOENIX_HARD_TIMEOUT"])

# Max number of simultaneous reboots (soft OR hard)
if "PHOENIX_MAX_REBOOTS" not in config:
    PHOENIX_MAX_REBOOTS = 20
else:
    PHOENIX_MAX_REBOOTS = int(config["PHOENIX_MAX_REBOOTS"])

# Timeout (s) for unix commands
if "PHOENIX_CMD_TIMEOUT" not in config:
    PHOENIX_CMD_TIMEOUT = 60
else:
    PHOENIX_CMD_TIMEOUT = int(config["PHOENIX_CMD_TIMEOUT"])

# Properties of the broken nodes (SQL where clause)
if "PHOENIX_BROKEN_NODES" not in config:
    PHOENIX_BROKEN_NODES = "state='Suspected' and network_address NOT IN (SELECT distinct(network_address) FROM resources where resource_id IN (SELECT resource_id  FROM assigned_resources WHERE assigned_resource_index = 'CURRENT'))"
else:
    PHOENIX_BROKEN_NODES = config["PHOENIX_BROKEN_NODES"]


# Set up logging
logging.basicConfig(filename=PHOENIX_LOGDIR + "/oar_phoenix.log", level=logging.INFO)


# Function to get a DB session on OAR DB
def wait_db() -> Any:
    try:
        session = init_and_get_session(config)
        wait_db_ready(get_alive_nodes_with_jobs, args=[session])
    except Exception as e:
        print(f"Failed to contact database: {e}")
        exit(1)
    return session


# Function to send a unix command with timeout and log date in the logfile
def send_cmd(cmd: str) -> Optional[str]:
    try:
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        process = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            stdout, stderr = process.communicate(timeout=PHOENIX_CMD_TIMEOUT)
            res = stdout.decode("utf-8") if stdout else stderr.decode("utf-8")
            if res:
                logging.info(f"{current_time} - {res}")
            else:
                logging.info(f"{current_time} - Command executed, no output")
        except subprocess.TimeoutExpired:
            logging.info(f"{current_time} - Command timed out!")
            process.kill()
            return "Timed out!"
    except Exception as e:
        return f"Exception occurred: {str(e)}"


# Load the PHOENIX_STATUS_FILE file
def load_status(file: str) -> Dict:
    with open(file, "r") as yamlfile:
        return yaml.safe_load(yamlfile)


# Export status to file
def save_status(file: str, ref: Dict) -> None:
    with open(file, "w") as yamlfile:
        yaml.dump(ref, yamlfile)


# Initialize STATUS file
def init_status(file: str) -> None:
    if not os.path.exists(file):
        with open(file, "w") as new_file:
            new_file.write("")  # Create an empty file if it doesn't exist

    if not os.path.getsize(file):
        empty_hash = {}
        save_status(file, empty_hash)


# Remove nodes that are no longer broken from DB
def clean_status(status: Dict, broken_nodes: List) -> None:
    broken_nodes = [node[0] for node in broken_nodes]
    for node in list(status):
        if node not in broken_nodes:
            del status[node]


# Get nodes to soft_reboot
def get_nodes_to_soft_reboot(status: Dict, broken_nodes: List) -> List:
    nodes = []
    c = 0
    for node in broken_nodes:
        if node[0] not in status:
            c += 1
            nodes.append(node[0])
        if c >= PHOENIX_MAX_REBOOTS:
            break
    return nodes


# Get nodes to hard_reboot
def get_nodes_to_hard_reboot(status: Dict, broken_nodes: List) -> List:
    nodes = []
    c = 0
    for node in broken_nodes:
        if node[0] in status and "soft_reboot" in status[node[0]]:
            if time.time() > status[node[0]]["soft_reboot"] + PHOENIX_SOFT_TIMEOUT:
                c += 1
                nodes.append(node[0])
        if c >= PHOENIX_MAX_REBOOTS:
            break
    return nodes


# Soft reboot nodes
def soft_reboot_nodes(status: Dict, nodes: List) -> None:
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    for node in nodes:
        logging.info(f"{current_time} - Soft rebooting the broken node {node}")
        cmd = PHOENIX_SOFT_REBOOTCMD.replace("{NODENAME}", node)
        status[node] = {"soft_reboot": time.time()}
        send_cmd(cmd)


# Hard reboot nodes
def hard_reboot_nodes(status: Dict, nodes: List) -> None:
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    for node in nodes:
        logging.info(f"{current_time} - Hard rebooting the broken node {node}")
        cmd = PHOENIX_HARD_REBOOTCMD.replace("{NODENAME}", node)
        del status[node]
        status[node] = {"hard_reboot": time.time()}
        send_cmd(cmd)


# Main function
def main() -> None:
    init_status(PHOENIX_STATUS_FILE)
    status = load_status(PHOENIX_STATUS_FILE)
    session = wait_db()
    broken_nodes = get_nodes_with_given_sql(session, PHOENIX_BROKEN_NODES)
    clean_status(status, broken_nodes)
    nodes = get_nodes_to_soft_reboot(status, broken_nodes)
    soft_reboot_nodes(status, nodes)
    nodes = get_nodes_to_hard_reboot(status, broken_nodes)
    hard_reboot_nodes(status, nodes)
    save_status(PHOENIX_STATUS_FILE, status)


if __name__ == "__main__":
    main()
