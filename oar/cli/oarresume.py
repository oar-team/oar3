# -*- coding: utf-8 -*-
"""oarresume - Resumes a job, it will be rescheduled."""

import click

import oar.lib.tools as tools
from oar import VERSION
from oar.lib.job_handling import (
    get_array_job_ids,
    get_job_ids_with_given_properties,
    resume_job,
)

from .utils import CommandReturns

click.disable_unicode_literals_warning = True


def oarresume(job_ids, array, sql, version, user=None, cli=True):
    cmd_ret = CommandReturns(cli)

    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        return cmd_ret

    if not job_ids and not sql and not array:
        cmd_ret.usage(1)
        return cmd_ret

    if array:
        job_ids = get_array_job_ids(array)

        if not job_ids:
            cmd_ret.warning("There are no job for this array job ({})".format(array), 4)

    if sql:
        job_ids = get_job_ids_with_given_properties(sql)
        if not job_ids:
            cmd_ret.warning(
                "There are no job for this SQL WHERE clause ({})".format(array), 4
            )

    for job_id in job_ids:
        error = resume_job(job_id, user)
        if error:
            error_msg = "/!\\ Cannot resume {} : ".format(job_id)
            if error == -1:
                error_msg += "this job does not exist."
                cmd_ret.error(error_msg, -1, 1)
            elif error == -2:
                error_msg += "you are not the right user."
                cmd_ret.error(error_msg, -2, 1)
            elif error == -3:
                error_msg += "the job is not in the Hold or Suspended state."
                cmd_ret.error(error_msg, -3, 1)
            elif error == -4:
                error_msg += "only oar or root user can resume Suspended jobs."
                cmd_ret.error(error_msg, -4, 1)
            else:
                error_msg += "unknown reason."
                cmd_ret.error(error_msg, 0, 1)
            return cmd_ret
        else:
            cmd_ret.print_(
                "[{}] Resume request was sent to the OAR server.".format(job_id)
            )

    tools.notify_almighty("ChState")

    return cmd_ret


@click.command()
@click.argument("job_id", nargs=-1)
@click.option("--array", type=int, help="Handle array job ids, and their sub-jobs")
@click.option(
    "--sql",
    type=click.STRING,
    help="Select jobs using a SQL WHERE clause on table jobs (e.g. \"project = 'p1'\")",
)
@click.option("-V", "--version", is_flag=True, help="Print OAR version.")
def cli(job_id, array, sql, version):
    """Ask OAR to change job_ids states into Waiting
    when it is Hold or in Running if it is Suspended."""

    cmd_ret = oarresume(job_id, array, sql, version, None)
    cmd_ret.exit()
