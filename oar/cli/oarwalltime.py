import os
import re

import click
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.walltime as walltime
from oar import VERSION
from oar.lib.globals import init_oar

from .utils import CommandReturns


def oarwalltime(
    session,
    config,
    job_id,
    new_walltime,
    force,
    delay_next_jobs,
    version,
    user=None,
    cli=True,
):
    cmd_ret = CommandReturns(cli)

    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        return cmd_ret

    if not job_id:
        cmd_ret.usage(1)
        return cmd_ret

    if (not new_walltime) and (delay_next_jobs or force):
        cmd_ret.error("New walltime argument is missing", 4, 1)
        return cmd_ret

    if new_walltime and (not re.search(r"^[-+]?\d+(?::\d+(?::\d+)?)?$", new_walltime)):
        cmd_ret.error(f"New walltime is malformatted: {new_walltime}", 4, 1)
        return cmd_ret

    if not new_walltime:
        (walltime_change, message, state) = walltime.get(session, config, job_id)
        # Job doesn't exist, or the feature is disabled
        if not walltime_change:
            cmd_ret.error(message, 2, 2)
            return cmd_ret

        if not walltime_change.walltime:
            cmd_ret.print_(
                "Walltime change status for job {} (job is not running yet):\n  N/A\n".format(
                    job_id
                )
            )
        else:
            # TODO
            granted_with = [  # noqa: F841
                s
                for s in (
                    "forced: " + walltime_change.granted_with_force,
                    "delaying next jobs: "
                    + walltime_change.granted_with_delay_next_jobs,
                )
                if not re.search(r": 0:0:0$", s)
            ]
            if state == "Running":
                cmd_ret.print_(
                    "Walltime change status for job {} (job is running):".format(job_id)
                )
                cmd_ret.print_(
                    "  Current walltime: {:>11}".format(walltime_change.walltime)
                )
                cmd_ret.print_(
                    "  Possible increase: {:>10}".format(walltime_change.possible)
                )
                cmd_ret.print_(
                    "  Already granted: {:>12}".format(walltime_change.granted)
                )
                cmd_ret.print_("  ({})".format(", ".granted_with))
                msg = ""
                if walltime_change.delay_next_jobs and (
                    walltime_change.delay_next_jobs == "YES"
                ):
                    msg = " (will possibly delay next jobs)"
                cmd_ret.print_(
                    "  Pending/unsatisfied: {:>8}{}".format(
                        walltime_change.pending, msg
                    )
                )
            else:
                cmd_ret.print_(
                    "Walltime change status for job {} (job is not running):".format(
                        job_id
                    )
                )
                cmd_ret.print_("  Walltime: {:>11}".format(walltime_change.walltime))
                cmd_ret.print_("  Granted: {:>12}".format(walltime_change.granted))
                cmd_ret.print_("  ({})".format(", ".granted_with))
                msg = ""
                if walltime_change.delay_next_jobs and (
                    walltime_change.delay_next_jobs == "YES"
                ):
                    msg = " (will possibly delay next jobs)"
                cmd_ret.print_(
                    "  Unsatisfied: {:>8}{}".format(walltime_change.pending, msg)
                )

        return cmd_ret

    # Request
    if not user:
        if "OARDO_USER" in os.environ:
            user = os.environ["OARDO_USER"]
        else:
            user = os.environ["USER"]

    (error, _, status, message) = walltime.request(
        session,
        config,
        job_id,
        user,
        new_walltime,
        "YES" if force else "NO",
        "YES" if delay_next_jobs else "NO",
    )
    # TODO error
    msg = "{}: {}".format(status.capitalize(), message)
    if error:
        cmd_ret.error(msg, error, error)
    else:
        cmd_ret.print_(msg)

    return cmd_ret


@click.command()
@click.argument("job_id", nargs=1, required=False, type=int)
@click.argument("new_walltime", nargs=1, required=False, type=click.STRING)
@click.option("--force", is_flag=True, help="request the change to apply at once")
@click.option(
    "--delay-next-jobs",
    is_flag=True,
    help="allow an extra time request to succeed even if it must delay other jobs, including other users' jobs",
)
@click.option("-V", "--version", is_flag=True, help="Print OAR version.")
def cli(job_id, new_walltime, force, delay_next_jobs, version):
    """Manage walltime change requests for a job.
    - If no new walltime is given, the command shows the current walltime change
      status for the job.
    - If a new walltime is given, the command requests a change of the walltime of the
      job, or update a previous request.

    The new walltime is to be passed in the format [+-]h:m:s. If no sign is used,
    The value is a new walltime absolute value (like passed to oarsub). If prefixed
    by +, the request is an increase of the walltime by the passed value. If
    prefixed by -, it is a decrease request. In that case the arguments need
    to be escaped with -- to avoid it to be parsed as a command-line option (e.g. oarwalltime -- <job_id> -1:0:0).

    The job must be running to request a walltime change.
    """
    ctx = click.get_current_context()
    cmd_ret = CommandReturns(cli)
    if ctx.obj:
        session, config = ctx.obj
    else:
        config, engine, log = init_oar()
        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        session = scoped()

    cmd_ret = oarwalltime(
        session, config, job_id, new_walltime, force, delay_next_jobs, version
    )
    cmd_ret.exit()
