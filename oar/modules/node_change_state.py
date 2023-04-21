#!/usr/bin/env python
# coding: utf-8
"""
This module is in charge of changing resource states and checking if there are
jobs on these.

It also checks all pending events in the table :ref:`database-event-logs-anchor`.
"""
import os
import re
import sys

from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools as tools
from oar.lib.database import EngineConnector
from oar.lib.event import (
    add_new_event,
    add_new_event_with_host,
    check_event,
    get_hostname_event,
    get_to_check_events,
    is_an_event_exists,
)
from oar.lib.globals import init_oar
from oar.lib.job_handling import (
    frag_job,
    get_cpuset_values,
    get_job,
    get_job_cpuset_name,
    get_job_host_log,
    get_job_types,
    is_job_already_resubmitted,
    resubmit_job,
    set_job_state,
    suspend_job_action,
)
from oar.lib.logging import get_logger
from oar.lib.node import get_all_resources_on_node, set_node_state
from oar.lib.queue import stop_all_queues
from oar.lib.resource_handling import (
    get_resource,
    get_resource_job_to_frag,
    get_resources_change_state,
    set_resource_nextState,
    set_resource_state,
)

_, _, logger = init_oar()

logger = get_logger(logger, "oar.modules.node_change_state", forward_stderr=True)
logger.info("Start Node Change State")


class NodeChangeState(object):
    def __init__(self, config):
        self.config = config
        self.exit_code = 0
        self.resources_to_heal = []
        self.cpuset_field = None
        if "JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD" in config:
            self.cpuset_field = config["JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD"]
        self.healing_exec_file = None
        if "SUSPECTED_HEALING_EXEC_FILE" in config:
            self.healing_exec_file = config["SUSPECTED_HEALING_EXEC_FILE"]

    def run(self, session):
        config = self.config
        for event in get_to_check_events(session):
            job_id = event.job_id
            logger.debug(
                "Check events for the job " + str(job_id) + " with type " + event.type
            )
            job = get_job(session, job_id)

            # Check if we must resubmit the idempotent jobs
            # User must specify that his job is idempotent and exit from hos script with the exit code 99.
            # So, after a successful checkpoint, if the job is resubmitted then all will go right
            # and there will have no problem (like file creation, deletion, ...).
            if (
                (event.type == "SWITCH_INTO_TERMINATE_STATE")
                or (event.type == "SWITCH_INTO_ERROR_STATE")
            ) and (job.exit_code and (job.exit_code >> 8) == 99):
                job_types = get_job_types(session, job_id)
                if "idempotent" in job_types.keys():
                    if (
                        job.reservation == "None"
                        and job.type == "PASSIVE"
                        and (not is_job_already_resubmitted(session, job_id))
                        and (is_an_event_exists(session, job_id, "SEND_KILL_JOB") == 0)
                        and ((job.stop_time - job.start_time) > 60)
                    ):
                        new_job_id = resubmit_job(session, job_id)
                        logger.warning(
                            "Resubmiting job "
                            + str(job_id)
                            + " => "
                            + str(new_job_id)
                            + "(type idempotent & exit code = 99 & duration > 60s)"
                        )

            #  Check if we must expressely change the job state
            if event.type == "SWITCH_INTO_TERMINATE_STATE":
                set_job_state(session, job_id, "Terminated")

            elif (event.type == "SWITCH_INTO_ERROR_STATE") or (
                event.type == "FORCE_TERMINATE_FINISHING_JOB"
            ):
                set_job_state(session, job_id, "Error")

            # Check if we must change the job state #
            type_to_check = [
                "PING_CHECKER_NODE_SUSPECTED",
                "CPUSET_ERROR",
                "PROLOGUE_ERROR",
                "CANNOT_WRITE_NODE_FILE",
                "CANNOT_WRITE_PID_FILE",
                "USER_SHELL",
                "EXTERMINATE_JOB",
                "CANNOT_CREATE_TMP_DIRECTORY",
                "LAUNCHING_OAREXEC_TIMEOUT",
                "RESERVATION_NO_NODE",
                "BAD_HASHTABLE_DUMP",
                "SSH_TRANSFER_TIMEOUT",
                "EXIT_VALUE_OAREXEC",
            ]

            if event.type in type_to_check:
                if (
                    (job.reservation == "None")
                    or (event.type == "RESERVATION_NO_NODE")
                    or (job.assigned_moldable_job == 0)
                ):
                    set_job_state(session, job_id, "Error")
                elif (
                    (job.reservation != "None")
                    and (event.type != "PING_CHECKER_NODE_SUSPECTED")
                    and (event.type != "CPUSET_ERROR")
                ):
                    set_job_state(session, job_id, "Error")

            if (event.type == "CPUSET_CLEAN_ERROR") or (event.type == "EPILOGUE_ERROR"):
                # At this point the job was executed normally
                # The state is changed here to avoid to schedule other jobs
                # on nodes that will be Suspected
                set_job_state(session, job_id, "Terminated")

            # Check if we must suspect some nodes
            type_to_check = [
                "PING_CHECKER_NODE_SUSPECTED",
                "PING_CHECKER_NODE_SUSPECTED_END_JOB",
                "CPUSET_ERROR",
                "CPUSET_CLEAN_ERROR",
                "SUSPEND_ERROR",
                "RESUME_ERROR",
                "PROLOGUE_ERROR",
                "EPILOGUE_ERROR",
                "CANNOT_WRITE_NODE_FILE",
                "CANNOT_WRITE_PID_FILE",
                "USER_SHELL",
                "EXTERMINATE_JOB",
                "CANNOT_CREATE_TMP_DIRECTORY",
                "SSH_TRANSFER_TIMEOUT",
                "BAD_HASHTABLE_DUMP",
                "LAUNCHING_OAREXEC_TIMEOUT",
                "EXIT_VALUE_OAREXEC",
                "FORCE_TERMINATE_FINISHING_JOB",
            ]
            type_to_check_cpuset_SR_error = [
                "CPUSET_ERROR",
                "CPUSET_CLEAN_ERROR",
                "SUSPEND_ERROR",
                "RESUME_ERROR",
            ]
            type_to_check_cpuset_LT_error = [
                "EXTERMINATE_JOB",
                "PROLOGUE_ERROR",
                "EPILOGUE_ERROR",
                "CPUSET_ERROR",
                "CPUSET_CLEAN_ERROR",
                "FORCE_TERMINATE_FINISHING_JOB",
            ]
            # import pdb; pdb.set_trace()
            if event.type in type_to_check:
                hosts = []
                finaud_tag = "NO"
                # Restrict Suspected state to the first node (node really connected with OAR)
                # for some event types
                if (
                    event.type == "PING_CHECKER_NODE_SUSPECTED"
                    or event.type == "PING_CHECKER_NODE_SUSPECTED_END_JOB"
                ):
                    hosts = get_hostname_event(session, event.id)
                    finaud_tag = "YES"
                elif event.type in type_to_check_cpuset_SR_error:
                    hosts = get_hostname_event(session, event.id)
                else:
                    hosts = get_job_host_log(session, job.assigned_moldable_job)
                    if event.type not in type_to_check_cpuset_LT_error:
                        hosts = [hosts[0]]
                    else:
                        # If we exterminate a job and the cpuset feature is configured
                        # then the CPUSET clean will tell us which nodes are dead
                        if ("JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD" in config) and (
                            event.type == "EXTERMINATE_JOB"
                        ):
                            hosts = []
                    add_new_event_with_host(
                        session, "LOG_SUSPECTED", 0, event.description, hosts
                    )

                if len(hosts) > 0:
                    already_treated_hosts = {}
                    for host in hosts:
                        if not ((host in already_treated_hosts) or (host == "")):
                            already_treated_hosts[host] = True
                            set_node_state(
                                session, host, "Suspected", finaud_tag, config
                            )
                            for resource_id in get_all_resources_on_node(session, host):
                                self.resources_to_heal.append(
                                    str(resource_id) + " " + host
                                )
                            self.exit_code = 1
                    msg = (
                        "Set nodes to suspected after error "
                        + event.type
                        + " "
                        + ",".join(hosts)
                    )
                    logger.warning(msg)
                    tools.send_log_by_email("Suspecting nodes", msg)

            # Check if we must stop the scheduling
            type_to_check = [
                "SERVER_PROLOGUE_TIMEOUT",
                "SERVER_PROLOGUE_EXIT_CODE_ERROR",
                "SERVER_EPILOGUE_TIMEOUT",
                "SERVER_EPILOGUE_EXIT_CODE_ERROR",
            ]
            if event.type in type_to_check:
                logger.warning(
                    "Server admin script error, stopping all scheduler queues: "
                    + event.type
                )
                tools.send_log_by_email(
                    "Stop all scheduling queues",
                    "Server admin script error, stopping all scheduler queues: "
                    + event.type
                    + ". Fix errors and run `oarnotify -E' to re-enable them.",
                )
                stop_all_queues(
                    session,
                )
                set_job_state(session, job_id, "Error")

            # Check if we must resubmit the job
            type_to_check = [
                "SERVER_PROLOGUE_TIMEOUT",
                "SERVER_PROLOGUE_EXIT_CODE_ERROR",
                "SERVER_EPILOGUE_TIMEOUT",
                "PING_CHECKER_NODE_SUSPECTED",
                "CPUSET_ERROR",
                "PROLOGUE_ERROR",
                "CANNOT_WRITE_NODE_FILE",
                "CANNOT_WRITE_PID_FILE",
                "USER_SHELL",
                "CANNOT_CREATE_TMP_DIRECTORY",
                "LAUNCHING_OAREXEC_TIMEOUT",
            ]
            if event.type in type_to_check:
                if (
                    job.reservation == "None"
                    and job.type == "PASSIVE"
                    and (is_job_already_resubmitted(session, job_id) == 0)
                ):
                    new_job_id = resubmit_job(session, job_id)
                    msg = (
                        "Resubmiting job "
                        + str(job_id)
                        + " => "
                        + str(new_job_id)
                        + " (due to event "
                        + event.type
                        + " & job is neither a reservation nor an interactive job)"
                    )
                    logger.warning(msg)
                    add_new_event(session, "RESUBMIT_JOB_AUTOMATICALLY", job_id, msg)

            # Check Suspend/Resume job feature
            if event.type in ["HOLD_WAITING_JOB", "HOLD_RUNNING_JOB", "RESUME_JOB"]:
                if event.type != "RESUME_JOB" and job.state == "Waiting":
                    set_job_state(session, job_id, "Hold")
                    if job.type == "INTERACTIVE":
                        addr, port = job.info_type.split(":")
                        tools.notify_tcp_socket(
                            addr, port, "Start prediction: undefined (Hold)"
                        )

                elif event.type != "RESUME_JOB" and job.state == "Resuming":
                    set_job_state(session, job_id, "Suspended")
                    tools.notify_almighty("Term")

                elif event.type == "HOLD_WAITING_JOB" and job.state == "Running":
                    job_types = get_job_types(session, job_id)
                    if "noop" in job_types.keys():
                        suspend_job_action(session, job_id, job.assigned_moldable_job)
                        logger.debug(str(job_id) + " suspend job of type noop")
                        tools.notify_almighty("Term")
                    else:
                        # Launch suspend command on all nodes
                        self.suspend_job(session, job, event)

                elif event.type == "RESUME_JOB" and job.state == "Suspend":
                    set_job_state(session, job_id, "Resuming")
                    tools.notify_almighty("Qresume")

                elif event.type == "RESUME_JOB" and job.state == "Hold":
                    set_job_state(session, job_id, "Waiting")
                    tools.notify_almighty("Qresume")

            # Check if we must notify the user
            if event.type == "FRAG_JOB_REQUEST":
                tools.notify_user(
                    job,
                    "INFO",
                    "Your job was asked to be deleted - " + event.description,
                )

            check_event(session, event.type, job_id)

        # Treate nextState field
        resources_to_change = get_resources_change_state(session)
        # A Term command must be added in the Almighty
        debug_info = {}
        if resources_to_change:
            self.exit_code = 1
            for r_id, next_state in resources_to_change.items():
                resource = get_resource(session, r_id)
                if resource.state != next_state:
                    set_resource_state(
                        session, r_id, next_state, resource.next_finaud_decision
                    )
                    set_resource_nextState(session, r_id, "UnChanged")

                    if resource.network_address not in debug_info:
                        debug_info[resource.network_address] = {}
                    debug_info[resource.network_address][r_id] = next_state

                    if next_state == "Suspected":
                        self.resources_to_heal.append(
                            str(r_id) + " " + resource.network_address
                        )

                    if (next_state == "Dead") or (next_state == "Absent"):
                        job_ids = get_resource_job_to_frag(session, r_id)
                        for job_id in job_ids:
                            logger.debug(
                                resource.network_address
                                + ": must kill job "
                                + str(job_id)
                            )
                            frag_job(session, job_id)
                            self.exit_code = 2

                else:
                    logger.debug(
                        "("
                        + resource.network_address
                        + ") "
                        + str(r_id)
                        + "is already in the "
                        + next_state
                        + " state"
                    )
                    set_resource_nextState(session, r_id, "UnChanged")

        email = None
        for network_address, rid_next_state in debug_info.items():
            str_mail = (
                "state change requested for "
                + network_address
                + ": "
                + str(rid_next_state)
            )
            logger.warning(str_mail)
            email = "[NodeChangeState] " + str_mail

        if email:
            tools.send_log_by_email("Resource state modifications", email)

        # FIXME: Not tested.
        timeout_cmd = config["SUSPECTED_HEALING_TIMEOUT"]
        healing_exec_file = config["SUSPECTED_HEALING_EXEC_FILE"]
        if healing_exec_file and len(self.resources_to_heal) > 0:
            logger.warning("Running healing script for suspected resources.")
            if tools.fork_and_feed_stdin(
                healing_exec_file, timeout_cmd, self.resources_to_heal
            ):
                logger.error(
                    " Try to launch the command $Healing_exec_file to heal resources, but the command timed out("
                    + " "
                    + str(timeout_cmd)
                    + " s)."
                )

    def suspend_job(self, session, job, event):
        config = self.config
        # SUSPEND PART

        if self.cpuset_field:
            cpuset_name = get_job_cpuset_name(session, job.id, job)
            nodes_cpuset_fields = get_cpuset_values(
                session, self.cpuset_field, job.assigned_moldable_job
            )

            suspend_data = {
                "name": cpuset_name,
                "job_id": job.id,
                "oarexec_pid_file": tools.get_oar_pid_file_name(job.id),
            }

            if nodes_cpuset_fields:
                taktuk_cmd = config["TAKTUK_CMD"]
                openssh_cmd = config["OPENSSH_CMD"]
                # TODO: TOREMOVE ?
                # if 'OAR_SSH_CONNECTION_TIMEOUT':
                #    tools.set_ssh_timeout(config['OAR_SSH_CONNECTION_TIMEOUT'])
                if "SUSPEND_RESUME_FILE" not in config:
                    msg = "SUSPEND_RESUME_FILE variable conguration is missing"
                    logger.error(msg)
                    raise Exception(msg)

                suspend_file = config["SUSPEND_RESUME_FILE"]
                if not re.match(r"^\/", suspend_file):
                    if "OARDIR" not in os.environ:
                        msg = "$OARDIR variable envionment must be defined"
                        logger.error(msg)
                        raise Exception(msg)
                    suspend_file = os.environ["OARDIR"] + "/" + suspend_file

                tag, bad = tools.manage_remote_commands(
                    nodes_cpuset_fields.keys(),
                    suspend_data,
                    suspend_file,
                    "suspend",
                    openssh_cmd,
                    taktuk_cmd,
                )
                # import pdb; pdb.set_trace()
                if tag == 0:
                    msg = (
                        "[SUSPEND_RESUME] ["
                        + str(job.id)
                        + "] Bad suspend/resume file: "
                        + suspend_file
                    )
                    logger.error(msg)
                    add_new_event(
                        session,
                        "SUSPEND_RESUME_MANAGER_FILE",
                        job.id,
                        "[NodeChangeState] " + msg,
                    )
                else:
                    if len(bad) == 0:
                        suspend_job_action(session, job.id, job.assigned_moldable_job)
                        suspend_script = None
                        if "JUST_AFTER_SUSPEND_EXEC_FILE" in config:
                            suspend_script = config["JUST_AFTER_SUSPEND_EXEC_FILE"]
                        timeout = config["SUSPEND_RESUME_SCRIPT_TIMEOUT"]
                        if suspend_script:
                            # Launch admin script
                            error_msg = tools.exec_with_timeout(
                                [suspend_script, str(job.id)], timeout
                            )

                            if error_msg:
                                msg = (
                                    "["
                                    + str(job.id)
                                    + "] suspend script error, job will resume:"
                                    + error_msg
                                )
                                tools.send_log_by_email("Suspend script error", msg)
                                add_new_event(
                                    session, "SUSPEND_SCRIPT_ERROR", job.id, msg
                                )
                                set_job_state(session, job.id, "Resuming")
                                tools.notify_almighty("Qresume")
                    else:
                        msg = (
                            "[SUSPEND_RESUME] ["
                            + str(job.id)
                            + "] error on several nodes: "
                            + str(bad)
                        )
                        logger.error(msg)
                        add_new_event_with_host(
                            session,
                            "SUSPEND_ERROR",
                            job.id,
                            "[NodeChangeState] " + msg,
                            bad,
                        )
                        frag_job(session, job.id)
                        # A Leon must be run
                        self.exit_code = 2
            tools.notify_almighty("Term")


def main():
    config, db, logger = init_oar()
    engine = EngineConnector(db).get_engine()

    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    logger = get_logger(logger, "oar.modules.sarko", forward_stderr=True)
    logger.info("Start Sarko")

    node_change_state = NodeChangeState(config)

    node_change_state.run(scoped)
    return node_change_state.exit_code


if __name__ == "__main__":  # pragma: no cover
    exit_code = main()
    sys.exit(exit_code)
