#!/usr/bin/env python
# coding: utf-8
"""
Bipbip is responsible for starting job when the scheduling decision has been taken.

The job execution is handled by the program OAREXEC executed by bipbip.
If a prologue script is defined, bipbib executes it before.
"""
import os
import re
import socket
import sys

import oar.lib.tools as tools
from oar.lib.event import add_new_event, add_new_event_with_host
from oar.lib.job_handling import (
    archive_some_moldable_job_nodes,
    check_end_of_job,
    get_cpuset_values,
    get_current_moldable_job,
    get_job,
    get_job_challenge,
    get_job_cpuset_name,
    get_job_current_hostnames,
    get_job_types,
    set_job_message,
    set_job_state,
)
from oar.lib.resource_handling import get_current_assigned_job_resources
from oar.lib.tools import (
    TimeoutExpired,
    format_ssh_pub_key,
    get_private_ssh_key_file_name,
    limited_dict2hash_perl,
    resources2dump_perl,
)
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger

config, db, logger = init_oar()

logger = get_logger(logger, "oar.modules.bipbip", forward_stderr=True)


class BipBip(object):
    def __init__(self, args):
        self.job_id = None
        if not args:
            self.exit_code = 1
            return
        self.job_id = int(args[0])

        self.server_prologue = config["SERVER_PROLOGUE_EXEC_FILE"]
        self.server_epilogue = config["SERVER_EPILOGUE_EXEC_FILE"]

        self.exit_code = 0

        self.oarexec_reattach_exit_value = None
        self.oarexec_reattach_script_exit_value = None
        self.oarexec_challenge = None
        if len(args) >= 2:
            self.oarexec_reattach_exit_value = args[1]
        if len(args) >= 3:
            self.oarexec_reattach_script_exit_value = args[2]
        if len(args) >= 4:
            self.oarexec_challenge = args[3]

    def run(self):
        job_id = self.job_id
        if not job_id:
            self.exit_code = 1
            return

        openssh_cmd = config["OPENSSH_CMD"]

        node_file_db_field = config["NODE_FILE_DB_FIELD"]
        node_file_db_field_distinct_values = config[
            "NODE_FILE_DB_FIELD_DISTINCT_VALUES"
        ]

        cpuset_field = ""
        cpuset_name = ""
        if "JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD" in config:
            cpuset_field = config["JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD"]
            cpuset_name = get_job_cpuset_name(job_id)

        cpuset_file = config["JOB_RESOURCE_MANAGER_FILE"]
        if not re.match(r"^\/", cpuset_file):
            if "OARDIR" not in os.environ:
                msg = "$OARDIR variable environment must be defined"
                logger.error(msg)
                raise Exception(msg)
            cpuset_file = os.environ["OARDIR"] + "/" + cpuset_file

        cpuset_full_path = ""
        cpuset_path = config["CPUSET_PATH"]
        if cpuset_path and cpuset_name:
            cpuset_full_path = cpuset_path + "/" + cpuset_name

        job_challenge, ssh_private_key, ssh_public_key = get_job_challenge(job_id)

        hosts = get_job_current_hostnames(job_id)
        job = get_job(job_id)

        # Check if we must treate the end of a oarexec
        if self.oarexec_reattach_exit_value and job.state in [
            "Launching",
            "Running",
            "Suspended",
            "Resuming",
        ]:
            logger.debug(
                "["
                + str(job.id)
                + "] OAREXEC end: "
                + self.oarexec_reattach_exit_value
                + " "
                + self.oarexec_reattach_script_exit_value
            )

            try:
                self.oarexec_reattach_exit_vint = int(self.oarexec_reattach_exit_value)

            except ValueError:
                logger.error(
                    "["
                    + str(job.id)
                    + "] Bad argument for bipbip : "
                    + self.oarexec_reattach_exit_value
                )
                self.exit_code = 2
                return

            if self.oarexec_challenge == job_challenge:
                check_end_of_job(
                    job_id,
                    self.oarexec_reattach_script_exit_value,
                    self.oarexec_reattach_exit_vint,
                    hosts,
                    job.user,
                    job.launching_directory,
                    self.server_epilogue,
                )
                return
            else:
                msg = (
                    "Bad challenge from oarexec, perhaps a pirate attack??? ("
                    + self.oarexec_challenge
                    + "/"
                    + job_challenge
                    + ")."
                )
                logger.error("[" + str(job.id) + "] " + msg)
                add_new_event("BIPBIP_CHALLENGE", job_id, msg)
                self.exit_code = 2
                return

        if job.state == "toLaunch":
            # Tell that the launching process is initiated
            set_job_state(job_id, "Launching")
            job.state = "Launching"
        else:
            logger.warning(
                "[" + str(job.id) + "] Job already treated or deleted in the meantime"
            )
            self.exit_code = 1
            return

        resources = get_current_assigned_job_resources(job.assigned_moldable_job)
        resources_data_str = ", 'resources' => " + resources2dump_perl(resources) + "}"

        mold_job_description = get_current_moldable_job(job.assigned_moldable_job)

        job_types = get_job_types(job.id)

        # HERE we must launch oarexec on the first node
        logger.debug(
            "["
            + str(job.id)
            + "] User: "
            + job.user
            + "; Command: "
            + job.command
            + " ==> hosts : "
            + str(hosts)
        )

        if (job.type == "INTERACTIVE") and (job.reservation == "None"):
            tools.notify_interactif_user(job, "Starting...")

        if (
            ("deploy" not in job_types.keys())
            and ("cosystem" not in job_types.keys())
            and (len(hosts) > 0)
        ):
            bad = []
            event_type = ""
            ###############
            # CPUSET PART #
            ###############
            nodes_cpuset_fields = None
            if cpuset_field:
                nodes_cpuset_fields = get_cpuset_values(
                    cpuset_field, job.assigned_moldable_job
                )

            if nodes_cpuset_fields and len(nodes_cpuset_fields) > 0:
                ssh_public_key = format_ssh_pub_key(
                    ssh_public_key, cpuset_full_path, job.user, job.user
                )
                cpuset_data_hash = {
                    "job_id": job.id,
                    "name": cpuset_name,
                    "nodes": nodes_cpuset_fields,
                    "cpuset_path": cpuset_path,
                    "ssh_keys": {
                        "public": {
                            "file_name": config["OAR_SSH_AUTHORIZED_KEYS_FILE"],
                            "key": ssh_public_key,
                        },
                        "private": {
                            "file_name": get_private_ssh_key_file_name(cpuset_name),
                            "key": ssh_private_key,
                        },
                    },
                    "oar_tmp_directory": config["OAREXEC_DIRECTORY"],
                    "user": job.user,
                    "job_user": job.user,
                    "node_file_db_fields": node_file_db_field,
                    "node_file_db_fields_distinct_values": node_file_db_field_distinct_values,
                    "array_id": job.array_id,
                    "array_index": job.array_index,
                    "stdout_file": job.stdout_file.replace("%jobid%", str(job.id)),
                    "stderr_file": job.stderr_file.replace("%jobid%", str(job.id)),
                    "launching_directory": job.launching_directory,
                    "job_name": job.name,
                    "types": job_types,
                    "walltime_seconds": "undef",
                    "walltime": "undef",
                    "project": job.project,
                    "log_level": config["LOG_LEVEL"],
                }

                taktuk_cmd = config["TAKTUK_CMD"]

                cpuset_data_str = limited_dict2hash_perl(cpuset_data_hash)
                cpuset_data_str = cpuset_data_str[:-1] + resources_data_str
                tag, bad_hosts = tools.manage_remote_commands(
                    nodes_cpuset_fields.keys(),
                    cpuset_data_str,
                    cpuset_file,
                    "init",
                    openssh_cmd,
                    taktuk_cmd,
                )
                if tag == 0:
                    msg = (
                        "[JOB INITIATING SEQUENCE] [CPUSET] ["
                        + str(job.id)
                        + "] Bad cpuset file: "
                        + cpuset_file
                    )
                    logger.error(msg)
                    events.append(("CPUSET_MANAGER_FILE", msg, None))
                else:
                    bad = bad + bad_hosts
                    event_type = "CPUSET_ERROR"
                    # Clean already configured cpuset
                    tmp_array = nodes_cpuset_fields.keys()
                    if (len(bad) > 0) and (len(tmp_array) > len(bad)):
                        # Verify if the job is a reservation
                        if job.reservation != "None":
                            # Look at if there is at least one alive node for the reservation
                            tmp_hosts = [h for h in hosts if h not in bad]
                            set_job_message(
                                job_id,
                                "One or several nodes are not responding correctly(CPUSET_ERROR)",
                            )

                            add_new_event_with_host(
                                event_type,
                                job_id,
                                "[bipbip] OAR cpuset suspects nodes for the job "
                                + str(job_id)
                                + ": "
                                + str(bad),
                                bad,
                            )
                            archive_some_moldable_job_nodes(
                                job.assigned_moldable_job, bad
                            )
                            tools.notify_almighty("ChState")
                            hosts = tmp_hosts
                            bad = []
                        else:
                            # Remove non initialized nodes
                            for h in bad:
                                nodes_cpuset_fields.pop(h)
                            # Regenerate cpuset_data_str w/ new nodes
                            cpuset_data_str = limited_dict2hash_perl(cpuset_data_hash)
                            cpuset_data_str = cpuset_data_str[:-1] + resources_data_str
                            tag, bad_hosts = tools.manage_remote_commands(
                                nodes_cpuset_fields.keys(),
                                cpuset_data_str,
                                cpuset_file,
                                "clean",
                                openssh_cmd,
                                taktuk_cmd,
                            )
                            bad = bad + bad_hosts

            #####################
            # CPUSET PART, END  #
            #####################

            # Check nodes
            if len(bad) > 0:
                reason = "OAR suspects nodes"

            if bad == []:
                # check nodes
                logger.debug("[" + str(job.id) + "] Check nodes: " + str(hosts))
                event_type = "PING_CHECKER_NODE_SUSPECTED"

                (pingcheck, bad) = tools.pingchecker(hosts)
                if not pingcheck:
                    bad = hosts
                    reason = "timeout triggered"
                elif len(bad) > 0:
                    reason = "OAR suspects nodes"

            if len(bad) > 0:
                set_job_message(
                    job_id, "One or several nodes are not responding correctly"
                )
                logger.error(
                    "["
                    + str(job.id)
                    + "] Some nodes are inaccessible ("
                    + event_type
                    + "):\n"
                    + str(bad)
                )
                exit_bipbip = 1
                if (job.type == "INTERACTIVE") and (job.reservation == "None"):
                    tools.notify_interactif_user(
                        job, "ERROR: some resources did not respond"
                    )
                else:
                    # Verify if the job is a reservation
                    if job.reservation != "None":
                        # Look at if there is at least one alive node for the reservation
                        tmp_hosts = [h for h in hosts if h not in bad]
                        if tmp_hosts == []:
                            add_new_event(
                                "RESERVATION_NO_NODE",
                                job_id,
                                "There is no alive node for the reservation "
                                + str(job_id)
                                + ".",
                            )
                        else:
                            exit_bipbip = 0

                add_new_event_with_host(
                    event_type,
                    job_id,
                    "[bipbip] "
                    + reason
                    + ", job: "
                    + str(job_id)
                    + ", nodes:"
                    + str(bad),
                    bad,
                )
                tools.notify_almighty("ChState")
                if exit_bipbip == 1:
                    self.exit_code = 2
                    return
            # end CHECK

        self.call_server_prologue(job)

        # CALL OAREXEC ON THE FIRST NODE
        pro_epi_timeout = config["PROLOGUE_EPILOGUE_TIMEOUT"]
        prologue_exec_file = config["PROLOGUE_EXEC_FILE"]
        epilogue_exec_file = config["EPILOGUE_EXEC_FILE"]

        modules_dir, _ = os.path.split(__file__)

        oarexec_files = (
            os.path.join(modules_dir, "../tools/Tools.pm"),
            os.path.join(modules_dir, "../tools/oarexec"),
        )

        head_node = None
        if hosts:
            head_node = hosts[0]

        # deploy, cosystem and no host part
        if ("cosystem" in job_types.keys()) or (len(hosts) == 0):
            head_node = config["COSYSTEM_HOSTNAME"]
        elif "deploy" in job_types.keys():
            head_node = config["DEPLOY_HOSTNAME"]

        almighty_hostname = config["SERVER_HOSTNAME"]
        if re.match(r"\s*localhost.*$", almighty_hostname) or re.match(
            r"^\s*127.*$", almighty_hostname
        ):
            almighty_hostname = socket.gethostname()

        logger.debug("[" + str(job.id) + "] Execute oarexec on node: " + head_node)

        job_challenge, _, _ = get_job_challenge(job_id)

        oarexec_cpuset_path = ""
        if (
            cpuset_full_path
            and ("cosystem" not in job_types.keys())
            and ("deploy" not in job_types.keys())
            and (len(hosts) > 0)
        ):
            # So oarexec will retry several times to contact Almighty until it will be
            # killed by the cpuset manager
            oarexec_cpuset_path = cpuset_full_path

        if node_file_db_field_distinct_values == "resource_id":
            node_file_db_field_distinct_values = "id"

        data_to_transfer = {
            "job_id": job.id,
            "array_id": job.array_id,
            "array_index": job.array_index,
            "stdout_file": job.stdout_file.replace("%jobid%", str(job.id)),
            "stderr_file": job.stderr_file.replace("%jobid%", str(job.id)),
            "launching_directory": job.launching_directory,
            "job_env": job.env,
            "node_file_db_fields": node_file_db_field,
            "node_file_db_fields_distinct_values": node_file_db_field_distinct_values,
            "user": job.user,
            "job_user": job.user,
            "types": job_types,
            "name": job.name,
            "project": job.project,
            "reservation": job.reservation,
            "walltime_seconds": mold_job_description.walltime,
            "command": job.command,
            "challenge": job_challenge,
            "almighty_hostname": almighty_hostname,
            "almighty_port": config["SERVER_PORT"],
            "checkpoint_signal": job.checkpoint_signal,
            "debug_mode": config["OAREXEC_DEBUG_MODE"],
            "mode": job.type,
            "pro_epi_timeout": pro_epi_timeout,
            "prologue": prologue_exec_file,
            "epilogue": epilogue_exec_file,
            "tmp_directory": config["OAREXEC_DIRECTORY"],
            "detach_oarexec": config["DETACH_JOB_FROM_SERVER"],
            "cpuset_full_path": oarexec_cpuset_path,
        }

        # print(data_to_transfer)
        # print(resources_data_str)
        data_to_transfer_str = limited_dict2hash_perl(data_to_transfer)
        data_to_transfer_str = data_to_transfer_str[:-1] + resources_data_str

        error = 50

        # timeout = pro_epi_timeout + config['BIPBIP_OAREXEC_HASHTABLE_SEND_TIMEOUT'] + config['TIMEOUT_SSH']
        cmd = openssh_cmd
        if (
            cpuset_full_path
            and ("cosystem" not in job_types.keys())
            and ("deploy" not in job_types.keys())
            and (len(hosts) > 0)
        ):
            # for oarsh_shell connection
            os.environ["OAR_CPUSET"] = cpuset_full_path
            cmd = cmd + " -oSendEnv=OAR_CPUSET "
        else:
            os.environ["OAR_CPUSET"] = ""

        cmd = cmd + " -x" + " -T " + head_node + " perl - " + str(job_id) + " OAREXEC"

        logger.debug(cmd)
        logger.debug(oarexec_files)

        def data_to_oar_env(data: dict[str, any]) -> dict[str, str]:
            """
            Simply transform the data supposed to be transferred to oarexec into a more OARish format for the server prologue environment.
            """
            new_env = dict()
            new_env["OAR_JOB_ID"] = str(data["job_id"])
            new_env["OAR_JOB_ARRAY_ID"] = str(data["array_id"])
            new_env["OAR_ARRAY_INDEX"] = str(data["array_index"])
            new_env["OAR_USER"] = str(data["user"])
            new_env["OAR_JOB_NAME"] = str(data["name"])
            new_env["OAR_JOB_WALLTIME_SECONDS"] = str(data["walltime_seconds"])
            new_env["OAR_JOB_COMMAND"] = str(data["command"])

            new_env["OAR_JOB_TYPES"] = ";".join(
                [
                    f"{k}=1" if v == True else f"{k}={v}"
                    for k, v in data["types"].items()
                ]
            )
            return new_env

        self.call_server_prologue(job, env=data_to_oar_env(data_to_transfer))

        # NOOP jobs
        if "noop" in job_types:
            set_job_state(job_id, "Running")
            logger.debug(
                "[" + str(job.id) + "] User: " + job.user + " Set NOOP job to Running"
            )
            return

        # ssh-oarexec exist error
        if tools.launch_oarexec(cmd, data_to_transfer_str, oarexec_files):
            set_job_state(job_id, "Running")
            # Notify interactive oarsub
            if (job.type == "INTERACTIVE") and (job.reservation == "None"):
                logger.debug(
                    "["
                    + str(job.id)
                    + "] Interactive request ;Answer to the client Qsub -I"
                )
                if not tools.notify_interactif_user(job, "GOOD JOB"):
                    addr, port = job.info_type.split(":")
                    logger.error(
                        "["
                        + str(job.id)
                        + "] Frag job because oarsub cannot be notified by the frontend on host {addr}:{port}. Check your network and firewall configuration\n".format(
                            addr=addr, port=port
                        )
                    )
                    tools.notify_almighty("Qdel")
                    return
            logger.debug("[" + str(job.id) + "] Exit from bipbip normally")
        else:
            # TODO: OAR3 only use detached OAREXEC
            #        child.expect('OAREXEC_SCRIPT_EXIT_VALUE\s*(\d+|N)', timeout=pro_epi_timeout)
            #        exit_script_value = child.match.group(1)
            #    except exceptions.TIMEOUT as e:
            #        pass
            if (job.type == "INTERACTIVE") and (job.reservation == "None"):
                tools.notify_interactif_user(
                    job, "ERROR: an error occured on the first job node"
                )

            check_end_of_job(
                job_id,
                self.oarexec_reattach_script_exit_value,
                error,
                hosts,
                job.user,
                job.launching_directory,
                self.server_epilogue,
            )
        return

    def call_server_prologue(self, job, env={}):
        # PROLOGUE EXECUTED ON OAR SERVER #
        # Script is executing with job id in arguments
        if self.server_prologue:
            timeout = config["SERVER_PROLOGUE_EPILOGUE_TIMEOUT"]
            cmd = [self.server_prologue, str(job.id)]

            try:
                child = tools.Popen(cmd, env=env)
                return_code = child.wait(timeout)

                if return_code:
                    msg = (
                        "["
                        + str(job.id)
                        + "] Server prologue exit code: "
                        + str(return_code)
                        + " (!=0) (cmd: "
                        + str(cmd)
                        + ")"
                    )
                    logger.error(msg)
                    add_new_event(
                        "SERVER_PROLOGUE_EXIT_CODE_ERROR", job.id, "[bipbip] " + msg
                    )
                    tools.notify_almighty("ChState")
                    if (job.type == "INTERACTIVE") and (job.reservation == "None"):
                        tools.notify_interactif_user(
                            job, "ERROR: SERVER PROLOGUE returned a bad value"
                        )
                    self.exit_code = 2
                    return 1

            except OSError as e:
                logger.error("Cannot run: {}. OsError: {}", str(cmd), e)

            except TimeoutExpired:
                tools.kill_child_processes(child.pid)
                msg = (
                    "[" + str(job.id) + "] Server prologue timeouted (cmd: " + str(cmd)
                )
                logger.error(msg)
                add_new_event("SERVER_PROLOGUE_TIMEOUT", job.id, "[bipbip] " + msg)
                tools.notify_almighty("ChState")
                if (job.type == "INTERACTIVE") and (job.reservation == "None"):
                    tools.notify_interactif_user(
                        job, "ERROR: SERVER PROLOGUE timeouted"
                    )
                self.exit_code = 2
                return 1

            return 0


def main():  # pragma: no cover
    if len(sys.argv) > 1:
        bipbip = BipBip(sys.argv[1:])
        try:
            bipbip.run()
        except Exception as ex:
            import traceback

            logger.error(
                "Bipbip.run trouble on job {}: {}\n{}".format(
                    sys.argv[1], ex, traceback.format_exc()
                )
            )

        return bipbip.exit_code
    else:
        return 1


if __name__ == "__main__":  # pragma: no cover
    exit_code = main()
    if exit_code:
        logger.error("Bipbip.run exit code is not null: {}".format(exit_code))
    sys.exit(exit_code)
