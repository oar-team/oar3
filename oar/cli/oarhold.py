# -*- coding: utf-8 -*-
"""oarhold - Put one or more jobs on hold. A waiting job which is put on hold is not scheduled for running. An already running job which is put on hold is suspended: processes are frozen (operation permitted to the administrator only)
"""

import click
from sqlalchemy.orm import scoped_session

import oar.lib.tools as tools
from oar import VERSION
from oar.lib.database import EngineConnector, sessionmaker
from oar.lib.globals import init_oar
from oar.lib.job_handling import (
    get_array_job_ids,
    get_job_ids_with_given_properties,
    get_job_types,
    hold_job,
)
from oar.lib.models import Model

from .utils import CommandReturns

click.disable_unicode_literals_warning = True


def oarhold(
    session, config, job_ids, running, array, sql, version, user=None, cli=True
):
    """Ask OAR to not schedule job_id until oarresume command will be executed."""
    cmd_ret = CommandReturns(cli)

    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        return cmd_ret

    if not job_ids and not sql and not array:
        cmd_ret.usage(1)
        return cmd_ret

    # import pdb; pdb.set_trace()
    if array:
        job_ids = get_array_job_ids(session, array)
        if not job_ids:
            cmd_ret.warning("There are no job for this array job ({})".format(array), 4)
            return cmd_ret

    if sql:
        job_ids = get_job_ids_with_given_properties(session, sql)
        if not job_ids:
            cmd_ret.warning(
                "There are no job for this SQL WHERE clause ({})".format(sql), 4
            )
            return cmd_ret

    for job_id in job_ids:
        if running:
            types = get_job_types(session, job_id)
            if "JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD" not in config:
                cmd_ret.warning("CPUSET tag is not configured in the oar.conf", 2, 2)
                return cmd_ret
            elif "cosystem" in types:
                cmd_ret.warning(
                    "This job is of the cosystem type. We cannot suspend this kind of jobs.",
                    3,
                    2,
                )
                return cmd_ret
            elif "deploy" in types:
                cmd_ret.warning(
                    "This job is of the deploy type. We cannot suspend this kind of jobs.",
                    4,
                    2,
                )
                return cmd_ret

        error = hold_job(session, config, job_id, running, user)
        if error:
            error_msg = "/!\\ Cannot hold {} : ".format(job_id)
            if error == -1:
                error_msg += "this job does not exist."
                cmd_ret.error(error_msg, -1, 1)
            elif error == -2:
                error_msg += "you are not the right user."
                cmd_ret.error(error_msg, -2, 1)
            elif error == -3:
                error_msg += "the job is not in the right state (try '-r' option)."
                cmd_ret.error(error_msg, -3, 1)
            elif error == -4:
                error_msg += "only oar or root users can use '-r' option."
                cmd_ret.error(error_msg, -4, 1)
            else:
                error_msg += "unknown reason."
                cmd_ret.error(error_msg, 0, 1)
            return cmd_ret
        else:
            cmd_ret.print_(
                "[{}] Hold request was sent to the OAR server.".format(job_id)
            )

    if job_ids:
        tools.notify_almighty("ChState")

    return cmd_ret


@click.command()
@click.argument("job_id", nargs=-1)
@click.option(
    "-r",
    "--running",
    is_flag=True,
    help="enable suspending running jobs (administrator only)",
)
@click.option("--array", type=int, help="Handle array job ids, and their sub-jobs")
@click.option(
    "--sql",
    type=click.STRING,
    help="Select jobs using a SQL WHERE clause on table jobs (e.g. \"project = 'p1'\")",
)
@click.option("-V", "--version", is_flag=True, help="Print OAR version.")
def cli(job_id, running, array, sql, version):
    ctx = click.get_current_context()
    if ctx.obj:
        (session, config) = ctx.obj
    else:
        config, db, log, session_factory = init_oar()
        engine = EngineConnector(db).get_engine()

        Model.metadata.drop_all(bind=engine)

        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        # TODO
        session = scoped()

    cmd_ret = oarhold(session, config, job_id, running, array, sql, version, None)
    cmd_ret.exit()
