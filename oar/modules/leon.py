#!/usr/bin/env python
# coding: utf-8
"""
This module is in charge to delete the jobs. Other OAR modules or commands
can ask to kill a job and this is Leon which performs that.

There are 2 frag types :

 - *normal* : Leon tries to connect to the first node allocated for the job and
   terminates the job.

 - *exterminate* : after a timeout if the *normal* method did not succeed
   then Leon notifies this case and clean up the database for these jobs. So
   OAR doesn't know what occured on the node and Suspects it.
"""
import sys

from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools as tools
from oar.lib.event import add_new_event
from oar.lib.globals import get_logger, init_oar
from oar.lib.job_handling import (
    get_job_current_hostnames,
    get_job_frag_state,
    get_job_types,
    get_jobs_to_kill,
    get_to_exterminate_jobs,
    job_arm_leon_timer,
    job_finishing_sequence,
    set_finish_date,
    set_job_message,
    set_job_state,
    set_running_date,
)

# config, db, log = init_oar(no_db=True)

logger = get_logger("oar.modules.leon", forward_stderr=True)
logger.info("Start Leon")


class Leon(object):
    def __init__(self, config, logger, args=None):
        self.args = args if args else []
        self.exit_code = 0
        self.logger = logger
        self.config = config

    def run(self, session):
        config = self.config
        logger = self.logger

        deploy_hostname = None
        if "DEPLOY_HOSTNAME" in config:
            deploy_hostname = config["DEPLOY_HOSTNAME"]

        cosystem_hostname = None

        if "COSYSTEM_HOSTNAME" in config:
            cosystem_hostname = config["COSYSTEM_HOSTNAME"]

        epilogue_script = None
        if "SERVER_EPILOGUE_EXEC_FILE" in config:
            epilogue_script = config["SERVER_EPILOGUE_EXEC_FILE"]

        openssh_cmd = config["OPENSSH_CMD"]

        # Test if we must launch a finishing sequence on a specific job
        if len(self.args) >= 1:
            try:
                job_id = int(self.args[0])
            except ValueError as ex:
                logger.error('"%s" cannot be converted to an int' % ex)
                self.exit_code = 1
                return

            frag_state = get_job_frag_state(session, job_id)

            if frag_state == "LEON_EXTERMINATE":
                # TODO: from leon.pl, do we need to ignore some signals
                # $SIG{PIPE} = 'IGNORE';
                # $SIG{USR1} = 'IGNORE';
                # $SIG{INT}  = 'IGNORE';
                # $SIG{TERM} = 'IGNORE';
                logger.debug('Leon was called to exterminate job "' + str(job_id) + '"')
                job_arm_leon_timer(session, job_id)
                events = [("EXTERMINATE_JOB", "I exterminate the job " + str(job_id))]
                job_finishing_sequence(session, config, epilogue_script, job_id, events)
                tools.notify_almighty("ChState")
            else:
                logger.error(
                    'Leon was called to exterminate job "'
                    + str(job_id)
                    + '" but its frag_state is not LEON_EXTERMINATE'
                )
            return

        for job in get_jobs_to_kill(
            session,
        ):
            # TODO pass if the job is job_desktop_computing one
            logger.debug("Normal kill: treates job " + str(job.id))
            if (job.state == "Waiting") or (job.state == "Hold"):
                logger.debug("Job is not launched")
                set_job_state(session, config, job.id, "Error")
                set_job_message(session, job.id, "Job killed by Leon directly")
                if job.type == "INTERACTIVE":
                    logger.debug("I notify oarsub in waiting mode")
                    addr, port = job.info_type.split(":")
                    if tools.notify_tcp_socket(addr, port, "JOB_KILLED"):
                        logger.debug("Notification done")
                    else:
                        logger.debug(
                            "Cannot open connection to oarsub client for job "
                            + str(job.id)
                            + ", it is normal if user typed Ctrl-C !"
                        )
                self.exit_code = 1
            elif (
                (job.state == "Terminated")
                or (job.state == "Error")
                or (job.state == "Finishing")
            ):
                logger.debug("Job is terminated or is terminating nothing to do")
            else:
                job_types = get_job_types(session, job.id)
                if "noop" in job_types.keys():
                    logger.debug("Kill the NOOP job: " + str(job.id))
                    set_finish_date(session, job)
                    set_job_state(session, config, job.id, "Terminated")
                    job_finishing_sequence(session, config,epilogue_script, job.id, [])
                    self.exit_code = 1
                else:
                    hosts = get_job_current_hostnames(session, job.id)
                    head_host = None
                    # deploy, cosystem and no host part
                    if ("cosystem" in job_types.keys()) or (len(hosts) == 0):
                        head_host = cosystem_hostname
                    elif "deploy" in job_types.keys():
                        head_host = deploy_hostname
                    elif len(hosts) != 0:
                        head_host = hosts[0]

                    if head_host:
                        add_new_event(
                            session,
                            "SEND_KILL_JOB",
                            job.id,
                            "Send the kill signal to oarexec on "
                            + head_host
                            + " for job "
                            + str(job.id),
                        )
                        comment = tools.signal_oarexec(
                            head_host, job.id, "TERM", False, openssh_cmd
                        )
                        logger.warning(comment)

            job_arm_leon_timer(session, job.id)

        # Treats jobs in state EXTERMINATED in the table fragJobs
        for job in get_to_exterminate_jobs(
            session,
        ):
            logger.debug("EXTERMINATE the job: " + str(job.id))
            set_job_state(session, config, job.id, "Finishing")
            if job.start_time == 0:
                set_running_date(session, job.id)
            set_finish_date(session, job)
            set_job_message(session, job.id, "Job exterminated by Leon")
            tools.notify_bipbip_commander(
                {"job_id": job.id, "cmd": "LEONEXTERMINATE", "args": []}
            )


def main():  # pragma: no cover
    config, engine, log = init_oar()

    leon = Leon(config, logger, sys.argv[1:])

    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)
    session = scoped()

    leon.run(session)
    return leon.exit_code


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
