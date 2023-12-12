#!/usr/bin/env python
import os
import subprocess
import time

import yaml

from oar.lib.database import wait_db_ready
from oar.lib.globals import init_and_get_session, init_config
from oar.lib.node import get_alive_nodes_with_jobs, get_nodes_with_given_sql

config = init_config()

# Get config variables and sets defaults if not defined into the oar.conf file

# File where phoenix saves it's state
if "PHOENIX_DBFILE" not in config:
    PHOENIX_DBFILE = "/var/lib/oar/phoenix/oar_phoenix.db"
else:
    PHOENIX_DBFILE = config["PHOENIX_DBFILE"]

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


# Function to get a DB session on OAR DB
def wait_db():
    try:
        session = init_and_get_session(config)
        wait_db_ready(get_alive_nodes_with_jobs, args=[session])
    except Exception as e:
        print(f"Failed to contact database: {e}")
        exit(1)
    return session


# Function to send a unix command with timeout and log date in the logfile
def send_cmd(cmd):
    try:
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(f"{PHOENIX_LOGDIR}/oar_phoenix.log", "a") as logfile:
            process = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            try:
                stdout, stderr = process.communicate(timeout=PHOENIX_CMD_TIMEOUT)
                res = stdout.decode("utf-8") if stdout else stderr.decode("utf-8")
                if res:
                    logfile.write(f"{current_time} - {res}\n")
                else:
                    logfile.write(f"{current_time} - Command executed, no output\n")
            except subprocess.TimeoutExpired:
                logfile.write(f"{current_time} - Command timed out!\n")
                process.kill()
                return "Timed out!"
    except Exception as e:
        return f"Exception occurred: {str(e)}"


# Load the DB file
def load_db(file):
    with open(file, "r") as yamlfile:
        return yaml.safe_load(yamlfile)


# Export DB to file
def save_db(file, ref):
    with open(file, "w") as yamlfile:
        yaml.dump(ref, yamlfile)


# Initialize DB file
def init_db(file):
    if not os.path.exists(file):
        with open(file, "w") as new_file:
            new_file.write("")  # Create an empty file if it doesn't exist

    if not os.path.getsize(file):
        empty_hash = {}
        save_db(file, empty_hash)


# Remove nodes that are no longer broken from DB
def clean_db(db, broken_nodes):
    broken_nodes = [node[0] for node in broken_nodes]
    for node in list(db):
        if node not in broken_nodes:
            del db[node]


# Get nodes to soft_reboot
def get_nodes_to_soft_reboot(db, broken_nodes):
    nodes = []
    c = 0
    for node in broken_nodes:
        if node[0] not in db:
            c += 1
            nodes.append(node[0])
        if c >= PHOENIX_MAX_REBOOTS:
            break
    return nodes


# Get nodes to hard_reboot
def get_nodes_to_hard_reboot(db, broken_nodes):
    nodes = []
    c = 0
    for node in broken_nodes:
        if node[0] in db and "soft_reboot" in db[node[0]]:
            if time.time() > db[node[0]]["soft_reboot"] + PHOENIX_SOFT_TIMEOUT:
                c += 1
                nodes.append(node[0])
        if c >= PHOENIX_MAX_REBOOTS:
            break
    return nodes


# Soft reboot nodes
def soft_reboot_nodes(db, nodes):
    logfile = open(f"{PHOENIX_LOGDIR}/oar_phoenix.log", "a")
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    for node in nodes:
        logfile.write(f"{current_time} - Soft rebooting the broken node {node}\n")
        logfile.close()
        cmd = PHOENIX_SOFT_REBOOTCMD.replace("{NODENAME}", node)
        db[node] = {"soft_reboot": time.time()}
        send_cmd(cmd)


# Hard reboot nodes
def hard_reboot_nodes(db, nodes):
    logfile = open(f"{PHOENIX_LOGDIR}/oar_phoenix.log", "a")
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    for node in nodes:
        logfile.write(f"{current_time} - Hard rebooting the broken node {node}\n")
        logfile.close()
        cmd = PHOENIX_HARD_REBOOTCMD.replace("{NODENAME}", node)
        del db[node]
        db[node] = {"hard_reboot": time.time()}
        send_cmd(cmd)


init_db(PHOENIX_DBFILE)
db = load_db(PHOENIX_DBFILE)
session = wait_db()
broken_nodes = get_nodes_with_given_sql(session, PHOENIX_BROKEN_NODES)
clean_db(db, broken_nodes)
nodes = get_nodes_to_soft_reboot(db, broken_nodes)
soft_reboot_nodes(db, nodes)
nodes = get_nodes_to_hard_reboot(db, broken_nodes)
hard_reboot_nodes(db, nodes)
save_db(PHOENIX_DBFILE, db)
