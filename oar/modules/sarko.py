#!/usr/bin/env python
# coding: utf-8
"""
This module is executed periodically by the Almighty (default is every
30 seconds).

The jobs of Sarko are:

 - Look at running job walltimes and ask to frag them if they had expired.
 - Detect if fragged jobs are really fragged otherwise asks to exterminate
   them.
 - In "Desktop Computing" mode, it detects if a node date has expired and asks to change its state into "Suspected".
 - Can change "Suspected" resources into "Dead" after :ref:`DEAD_SWITCH_TIME <DEAD_SWITCH_TIME>` seconds.

"""
import sys

import oar.lib.tools as tools
from oar.lib.event import add_new_event, add_new_event_with_host
from oar.lib.globals import get_logger, init_oar
from oar.lib.job_handling import (
    frag_job,
    get_current_moldable_job,
    get_frag_date,
    get_job_current_hostnames,
    get_job_suspended_sum_duration,
    get_job_types,
    get_jobs_in_state,
    get_timer_armed_job,
    job_fragged,
    job_leon_exterminate,
    job_refrag,
)
from oar.lib.resource_handling import (
    get_absent_suspected_resources_for_a_timeout,
    get_expired_resources,
    get_resource,
    set_resource_nextState,
    update_resource_nextFinaudDecision,
)


class Sarko(object):
    def __init__(self, config, logger):
        self.guilty_found = 0
        self.conf = config
        self.logger = logger

    def run(self, session):
        config = self.conf
        logger = self.logger

        leon_soft_walltime = config["LEON_SOFT_WALLTIME"]
        leon_walltime = config["LEON_WALLTIME"]

        if "JOBDEL_SOFTWALLTIME" in config:
            leon_soft_walltime = config["JOBDEL_SOFTWALLTIME"]

        if "JOBDEL_WALLTIME" in config:
            leon_walltime = config["JOBDEL_WALLTIME"]

        if leon_walltime <= leon_soft_walltime:
            leon_walltime = leon_soft_walltime + 10
            logger.warning(
                "(JOBDEL_WALLTIME <= JOBDEL_SOFTWALLTIME), changes JOBDEL_WALLTIME to "
                + str(leon_walltime)
            )

        deploy_hostname = None
        if "DEPLOY_HOSTNAME" in config:
            deploy_hostname = config["DEPLOY_HOSTNAME"]

        cosystem_hostname = None

        if "COSYSTEM_HOSTNAME" in config:
            cosystem_hostname = config["COSYSTEM_HOSTNAME"]

        openssh_cmd = config["OPENSSH_CMD"]

        logger.debug(
            "JOBDEL_SOFTWALLTIME = "
            + str(leon_soft_walltime)
            + "; JOBDEL_WALLTIME = "
            + str(leon_walltime)
        )

        logger.debug("Hello, identity control !!!")

        date = tools.get_date(session)

        # Look at leon timers
        # Decide if OAR must retry to delete the job or just change values in the database
        for job in get_timer_armed_job(session):
            if job.state in ["Terminated", "Error", "Finishing"]:
                job_fragged(session, job.id)
                logger.debug("Set to FRAGGED the job: " + str(job.id))
            else:
                frag_date = get_frag_date(session, job.id)
                if (date > (frag_date + leon_soft_walltime)) and (
                    date <= (frag_date + leon_walltime)
                ):
                    logger.debug("Leon will RE-FRAG bipbip of job :" + str(job.id))
                    job_refrag(session, job.id)
                    self.guilty_found = 1
                elif date > (frag_date + leon_walltime):
                    logger.debug("Leon will EXTERMINATE bipbip of job :" + str(job.id))
                    job_leon_exterminate(session, job.id)
                    self.guilty_found = 1
                else:
                    logger.debug(
                        "The leon timer is not yet expired for the job :"
                        + str(job.id)
                        + "; nothing to do"
                    )

        # Look at job walltimes
        for job in get_jobs_in_state(session, "Running"):
            start_time = job.start_time
            # Get walltime
            mold_job = get_current_moldable_job(session, job.assigned_moldable_job)
            max_time = mold_job.walltime
            if job.suspended == "YES":
                max_time = get_job_suspended_sum_duration(session, job.id, date)

            logger.debug(
                "Job: "
                + str(job.id)
                + " from "
                + str(start_time)
                + " with "
                + str(max_time)
                + "; current time="
                + str(date)
            )

            if date > (start_time + max_time):
                logger.debug("--> walltime reached")
                self.guilty_found = 1
                frag_job(session, job.id)
                add_new_event(
                    session,
                    "WALLTIME",
                    job.id,
                    "Job: "
                    + str(job.id)
                    + " from "
                    + str(start_time)
                    + " with "
                    + str(max_time)
                    + "; current time="
                    + str(date)
                    + " (Elapsed)",
                )
            elif (job.checkpoint > 0) and (
                date >= (start_time + max_time - job.checkpoint)
            ):
                # OAR must notify the job to checkpoint itself
                logger.debug("Send checkpoint signal to the job:" + str(job.id))
                # Retrieve node names used by the job
                hosts = get_job_current_hostnames(session, job.id)
                job_types = get_job_types(session, job.id)
                head_host = None
                # deploy, cosystem and no host part
                if ("cosystem" in job_types.keys()) or (len(hosts) == 0):
                    head_host = cosystem_hostname
                elif "deploy" in job_types.keys():
                    head_host = deploy_hostname
                elif len(hosts) != 0:
                    head_host = hosts[0]

                add_new_event(
                    session,
                    "CHECKPOINT",
                    job.id,
                    "User oar (sarko) requested a checkpoint on the job:"
                    + str(job.id)
                    + " on "
                    + head_host,
                )

                comment = tools.signal_oarexec(
                    head_host, job.id, "SIGUSR2", 1, openssh_cmd
                )
                if comment:
                    logger.warning(comment)
                    add_new_event(
                        session, "CHECKPOINT_ERROR", job.id, "[Sarko]" + comment
                    )
                else:
                    comment = (
                        "The job "
                        + str(job.id)
                        + " was notified to checkpoint itself on the node "
                        + head_host
                    )
                    logger.debug(comment)
                    add_new_event(
                        session, "CHECKPOINT_SUCCESSFULL", job.id, "[Sarko]" + comment
                    )

        # Retrieve nodes with expiry_dates in the past
        # special for Desktop computing (UNUSED ?)
        resource_ids = get_expired_resources(session)
        for resource_id in resource_ids:
            set_resource_nextState(session, resource_id, "Suspected")
            resource = get_resource(session, resource_id)
            add_new_event_with_host(
                session,
                "LOG_SUSPECTED",
                0,
                "The DESKTOP COMPUTING resource $r has expired on node "
                + resource.network_address,
                [resource.network_address],
            )

        if len(resource_ids) > 0:
            tools.notify_almighty("ChState")

        dead_switch_time = int(config["DEAD_SWITCH_TIME"])
        # Get Absent and Suspected nodes for more than 5 mn (default)
        if dead_switch_time > 0:
            notify = False
            for resource_id in get_absent_suspected_resources_for_a_timeout(
                session, dead_switch_time
            ):
                set_resource_nextState(session, resource_id, "Dead")
                update_resource_nextFinaudDecision(session, resource_id, "YES")

                logger.debug(
                    "Set the next state of resource: " + str(resource_id) + " to Dead"
                )
                notify = True

            if notify:
                tools.notify_almighty("ChState")


def main():  # pragma: no cover
    config, _, logger, _ = init_oar()

    logger = get_logger("oar.modules.sarko", forward_stderr=True)
    logger.info("Start Sarko")

    sarko = Sarko(config, logger)
    sarko.run()
    return sarko.guilty_found


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
