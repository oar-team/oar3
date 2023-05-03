# -*- coding: utf-8 -*-
"""oardel - delete or checkpoint job(s)."""
import os

import click
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools as tools
from oar import VERSION
from oar.lib.basequery import BaseQueryCollection
from oar.lib.database import EngineConnector
from oar.lib.globals import init_oar
from oar.lib.job_handling import (
    add_current_job_types,
    add_new_event,
    ask_checkpoint_signal_job,
    frag_job,
    get_array_job_ids,
    get_job,
    get_job_current_hostnames,
    get_job_duration_in_state,
    get_job_ids_with_given_properties,
    get_job_state,
    get_job_types,
    remove_current_job_types,
)
from oar.lib.models import Model
from oar.lib.resource_handling import update_current_scheduler_priority

from .utils import CommandReturns

click.disable_unicode_literals_warning = True


def oardel(
    session,
    config,
    job_ids,
    checkpoint,
    signal,
    besteffort,
    array,
    sql,
    force_terminate_finishing_job,
    version,
    user=None,
    cli=True,
):

    if not user:
        if "OARDO_USER" in os.environ:
            user = os.environ["OARDO_USER"]
        else:
            user = os.environ["USER"]

    cmd_ret = CommandReturns(cli)

    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        return cmd_ret

    if not job_ids and not sql and not array:
        cmd_ret.usage(1)
        return cmd_ret

    if array:
        job_ids = get_array_job_ids(session, array)

        if not job_ids:
            cmd_ret.warning("There are no job for this array job ({})".format(array), 4)
            return cmd_ret

    if sql:
        job_ids = get_job_ids_with_given_properties(session, sql)
        if not job_ids:
            cmd_ret.warning(
                "There are no job for this SQL WHERE clause ({})".format(array), 4
            )
            return cmd_ret

    if checkpoint or signal:
        for job_id in job_ids:
            if checkpoint:
                tag = "CHECKPOINT"
                cmd_ret.print_("Checkpointing the job {} ...".format(job_id))
            else:
                tag = "SIG"
                cmd_ret.print_(
                    "Signaling the job {} with {} signal {}.".format(
                        job_id, signal, user
                    )
                )

            error, error_msg = ask_checkpoint_signal_job(session, job_id, signal, user)

            if error > 0:
                cmd_ret.print_("ERROR")
                if error == 1:
                    cmd_ret.error(error_msg, error, 1)
                elif error == 3:
                    cmd_ret.error(error_msg, error, 7)
                else:
                    cmd_ret.error(error_msg, error, 5)
            else:
                # Retrieve hostnames used by the job
                hosts = get_job_current_hostnames(session, job_id)
                types = get_job_types(session, job_id)
                host_to_connect = None
                if "cosystem" in types:
                    host_to_connect = config["COSYSTEM_HOSTNAME"]
                elif "deploy" in types:
                    host_to_connect = config["DEPLOY_HOSTNAME"]
                else:
                    host_to_connect = hosts[0]
                timeout_ssh = config["OAR_SSH_CONNECTION_TIMEOUT"]

                error = tools.signal_oarexec(
                    host_to_connect,
                    job_id,
                    "SIGUSR2",
                    timeout_ssh,
                    config["OPENSSH_CMD"],
                    "",
                )
                if error != 0:
                    cmd_ret.print_("ERROR")
                    if error == 3:
                        comment = (
                            "Cannot contact {}, operation timouted ({} s).".format(
                                host_to_connect, timeout_ssh
                            )
                        )
                        cmd_ret.error(comment, 3, 3)
                    else:
                        comment = "An unknown error occured."
                        cmd_ret.error(comment, error, 1)
                    add_new_event(session, "{}_ERROR".format(tag), job_id, comment)
                else:
                    cmd_ret.print_("DONE")
                    comment = (
                        "The job {} was notified to checkpoint itself on {}.".format(
                            job_id, host_to_connect
                        )
                    )
                    add_new_event(session, "{}_SUCCESS".format(tag), job_id, comment)

    elif force_terminate_finishing_job:
        if not (user == "oar" or user == "root"):
            comment = "You must be oar or root to use the --force-terminate-finishing-job option"
            cmd_ret.error(comment, 1, 8)
        else:
            # $max_duration = 2 * OAR::Tools::get_taktuk_timeout()\
            # + OAR::Conf::get_conf_with_default_param("SERVER_PROLOGUE_EPILOGUE_TIMEOUT",0);
            max_duration = 2 * config["OAR_SSH_CONNECTION_TIMEOUT"] + int(
                config["SERVER_PROLOGUE_EPILOGUE_TIMEOUT"]
            )
            for job_id in job_ids:
                cmd_ret.print_(
                    "Force the termination of the job = {} ...".format(job_id)
                )
                if get_job_state(session, job_id) == "Finishing":
                    duration = get_job_duration_in_state(session, job_id, "Finishing")
                    if duration > max_duration:
                        comment = "Force to Terminate the job {} which is in Finishing state".format(
                            job_id
                        )
                        add_new_event(
                            session, "FORCE_TERMINATE_FINISHING_JOB", job_id, comment
                        )
                        cmd_ret.print_("REGISTERED.")
                    else:
                        error_msg = "The job {} is not in the Finishing state for more than {}s ({}s).".format(
                            job_id, max_duration, duration
                        )
                        cmd_ret.warning(error_msg, 1, 11)
                else:
                    error_msg = "The job {} is not in the Finishing state.".format(
                        job_id
                    )
                    cmd_ret.warning(error_msg, 1, 10)

            completed = tools.notify_almighty("ChState")
            if not completed:
                cmd_ret.error("Unable to notify Almighty", -1, 2)

    elif besteffort:
        if not (user == "oar" or user == "root"):
            comment = "You must be oar or root to use the --besteffort option"
            cmd_ret.error(comment, 1, 8)
        else:
            for job_id in job_ids:
                job = get_job(session, job_id)
                if job.state == "Running":
                    if "besteffort" in get_job_types(session, job_id):
                        update_current_scheduler_priority(
                            session, config, job, "-2", "STOP"
                        )
                        remove_current_job_types(session, job_id, "besteffort")
                        add_new_event(
                            session,
                            "DELETE_BESTEFFORT_JOB_TYPE",
                            job_id,
                            "User {} removed the besteffort type.".format(user),
                        )
                        cmd_ret.print_(
                            "Remove besteffort type for the job {}.".format(job_id)
                        )
                    else:
                        add_current_job_types(session, job_id, "besteffort")
                        update_current_scheduler_priority(
                            session, config, job, "+2", "START"
                        )
                        add_new_event(
                            session,
                            "ADD_BESTEFFORT_JOB_TYPE",
                            job_id,
                            "User {} added the besteffort type.".format(user),
                        )
                        cmd_ret.print_(
                            "Add besteffort type for the job {}.".format(job_id)
                        )
                else:
                    cmd_ret.warning(
                        "The job {} is not in the Running state.".format(job_id), 9
                    )

            completed = tools.notify_almighty("ChState")
            if not completed:
                cmd_ret.error("Unable to notify Almighty", -1, 2)

    else:
        # oardel is used to delete some jobs
        notify_almighty = False
        jobs_registred = []
        for job_id in job_ids:
            # TODO array of errors and error messages
            cmd_ret.info("Deleting the job = {} ...".format(job_id))
            error = frag_job(session, job_id)
            error_msg = ""
            if error == -1:
                error_msg = "Cannot frag {} ; You are not the right user.".format(
                    job_id
                )
                cmd_ret.error(error_msg, -1, 1)
            elif error == -2:
                error_msg = "Cannot frag {} ; This job was already killed.".format(
                    job_id
                )
                notify_almighty = True
                cmd_ret.warning(error_msg, -2, 6)
            elif error == -3:
                error_msg = "Cannot frag {} ; Job does not exist.".format(job_id)
                cmd_ret.warning(error_msg, -3, 7)
            else:
                cmd_ret.info(error_msg)
                notify_almighty = True
                jobs_registred.append(job_id)

        if notify_almighty:
            # Signal Almigthy
            # TODO: Send only Qdel ???? or ChState and Qdel in one message
            completed = tools.notify_almighty("ChState")
            if completed:
                tools.notify_almighty("Qdel")
                cmd_ret.info(
                    "The job(s) {}  will be deleted in the near future.".format(
                        jobs_registred
                    )
                )
            else:
                cmd_ret.error("Unable to notify Almighty", -1, 2)

    return cmd_ret


@click.command()
@click.argument("job_id", nargs=-1)
@click.option(
    "-c",
    "--checkpoint",
    is_flag=True,
    help='Send the checkpoint signal designed from the "--signal"\
              oarsub command option (default is SIGUSR2) to the process launched by the job "job_id".',
)
@click.option(
    "-s",
    "--signal",
    type=click.STRING,
    help="Send signal to the process launched by the selected jobs.",
)
@click.option(
    "-b",
    "--besteffort",
    is_flag=True,
    help="Change jobs to besteffort (or remove them if they are already besteffort)",
)
@click.option("--array", type=int, help="Handle array job ids, and their sub-jobs")
@click.option(
    "--sql",
    type=click.STRING,
    help="Select jobs using a SQL WHERE clause on table jobs (e.g. \"project = 'p1'\")",
)
@click.option(
    "--force-terminate-finishing-job",
    is_flag=True,
    help="Force jobs stuck in the Finishing state to switch to Terminated \
              (Warning: only use as a last resort). Using this option indicates \
              that something nasty happened, nodes where the jobs were executing will \
              subsequently be turned into Suspected.",
)
@click.option("-V", "--version", is_flag=True, help="Print OAR version.")
@click.pass_context
def cli(
    ctx,
    job_id,
    checkpoint,
    signal,
    besteffort,
    array,
    sql,
    force_terminate_finishing_job,
    version,
):
    """Kill or remove a job from the waiting queue."""
    ctx = click.get_current_context()
    cmd_ret = CommandReturns(cli)
    if ctx.obj:
        session, config = ctx.obj

    else:
        config, db, log, session_factory = init_oar()
        engine = EngineConnector(db).get_engine()

        Model.metadata.drop_all(bind=engine)

        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        # TODO
        session = scoped()

    cmd_ret = oardel(
        session,
        config,
        job_id,
        checkpoint,
        signal,
        besteffort,
        array,
        sql,
        force_terminate_finishing_job,
        version,
        None,
    )

    cmd_ret.exit()
