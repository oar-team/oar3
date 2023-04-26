# coding: utf-8
""" Functions to handle walltime change"""
# WALLTIME CHANGE MANAGEMENT
#

import re

import oar.lib.tools as tools
from oar.lib.globals import init_oar
from oar.lib.job_handling import (
    get_current_moldable_job,
    get_job,
    get_job_suspended_sum_duration,
    get_job_types,
)
from oar.lib.logging import get_logger
from oar.lib.models import WalltimeChange
from oar.lib.tools import duration_to_sql, duration_to_sql_signed, hms_to_duration

_, _, log = init_oar()

logger = get_logger(log, "oar.lib.walltime")


def get_conf(config_value, queue, walltime, value):
    value = config_value
    if isinstance(config_value, str) and (config_value != ""):
        # TODO add try / except
        conf_val = eval(config_value)
        if isinstance(conf_val, dict):
            if queue and queue in conf_val:
                value = conf_val[queue]
            elif "_" in conf_val:
                value = conf_val["_"]

    if walltime and isinstance(value, float) and value <= 1.0:
        value = int(walltime * value)

    return value


def get_walltime_change_for_job(session, job_id):
    """Get the current extra time added for a given job"""
    try:
        walltime_change = (
            session.query(WalltimeChange).filter(WalltimeChange.job_id == job_id).one()
        )
    except Exception as e:
        logger.debug(
            "get_walltime_change_for_job("
            + str(job_id)
            + ") raises exception: "
            + str(e)
        )
        return None
    else:
        return walltime_change


def get(session, config, job_id):
    if (
        "WALLTIME_CHANGE_ENABLED" not in config
        or config["WALLTIME_CHANGE_ENABLED"] != "YES"
    ):
        return (None, "functionality is disabled", None)

    job = get_job(session, job_id)
    if not job:
        return (None, "unknown job", None)

    walltime_change = get_walltime_change_for_job(session, job_id)

    if not walltime_change:
        walltime_change = WalltimeChange()

    if job.assigned_moldable_job != 0:
        moldable = get_current_moldable_job(session, job.assigned_moldable_job)
        walltime_change.walltime = moldable.moldable_walltime
    else:
        walltime_change.walltime = 0

    if not walltime_change.pending:
        walltime_change.pending = 0

    if not walltime_change.granted:
        walltime_change.granted = 0

    if not walltime_change.granted_with_force:
        walltime_change.granted_with_force = 0

    if not walltime_change.granted_with_delay_next_jobs:
        walltime_change.granted_with_delay_next_jobs = 0

    walltime_max_increase = get_conf(
        config["WALLTIME_MAX_INCREASE"],
        job.queue_name,
        walltime_change.walltime - walltime_change.granted,
        0,
    )

    walltime_min_for_change = get_conf(
        config["WALLTIME_MIN_FOR_CHANGE"], job.queue_name, None, 0
    )
    walltime_users_allowed_to_force = get_conf(
        config["WALLTIME_ALLOWED_USERS_TO_FORCE"], job.queue_name, None, ""
    )

    # TODO Unused
    walltime_users_allowed_to_delay_jobs = get_conf(  # noqa
        config["WALLTIME_ALLOWED_USERS_TO_DELAY_JOBS"], job.queue_name, None, ""
    )
    now = tools.get_date(session)

    # TODO Unused
    suspended = get_job_suspended_sum_duration(session, job_id, now)  # noqa

    if job.state != "Running" or (walltime_change.walltime < walltime_min_for_change):
        walltime_change.possible = duration_to_sql_signed(0)
    elif walltime_max_increase == -1:
        walltime_change.possible = "UNLIMITED"
    else:
        walltime_change.possible = duration_to_sql_signed(walltime_max_increase)

    if walltime_users_allowed_to_force != "*":
        search = re.compile(r"[,\s]+").split(walltime_users_allowed_to_force)
        q = re.compile(r"^{}$".format(job.user))
        if not any([re.search(q, s) for s in search]):
            walltime_change.force = "FORBIDDEN"
    elif (not walltime_change.force) or (walltime_change.pending == 0):
        walltime_change.force = "NO"

    if walltime_change.walltime != 0:
        walltime_change.walltime = duration_to_sql(walltime_change.walltime)
        walltime_change.pending = duration_to_sql_signed(walltime_change.pending)
        walltime_change.granted = duration_to_sql_signed(walltime_change.granted)
        walltime_change.granted_with_force = duration_to_sql_signed(
            walltime_change.granted_with_force
        )
        walltime_change.granted_with_delay_next_jobs = duration_to_sql_signed(
            walltime_change.granted_with_delay_next_jobs
        )
    else:
        # job is not running yet, walltime may not be known yet, in case of a moldable job
        walltime_change.walltime = None
        walltime_change.pending = None
        walltime_change.granted = None
        walltime_change.granted_with_force = None
        walltime_change.granted_with_delay_next_jobs = None

    return (walltime_change, None, job.state)


def request(session, config, job_id, user, new_walltime, force, delay_next_jobs):
    if (
        "WALLTIME_CHANGE_ENABLED" not in config
        or config["WALLTIME_CHANGE_ENABLED"] != "YES"
    ):
        return (5, 405, "not available", "functionality is disabled")

    job = get_job(session, job_id)

    if not job:
        return (4, 404, "not found", "could not find job {}".format(job_id))

    # Job user must be usr or root or oar
    if (job.user != user) and (user not in ("root", "oar")):
        return (3, 403, "forbidden", "job {} does not belong to you".format(job_id))

    # Job must be running
    if job.state != "Running":
        return (3, 403, "forbidden", "job {} is not running".format(job_id))

    moldable = get_current_moldable_job(session, job.assigned_moldable_job)

    walltime_max_increase = get_conf(
        config["WALLTIME_MAX_INCREASE"], job.queue_name, moldable.walltime, 0
    )
    walltime_min_for_change = get_conf(
        config["WALLTIME_MIN_FOR_CHANGE"], job.queue_name, None, 0
    )
    walltime_users_allowed_to_force = get_conf(
        config["WALLTIME_ALLOWED_USERS_TO_FORCE"], job.queue_name, None, ""
    )
    walltime_users_allowed_to_delay_jobs = get_conf(
        config["WALLTIME_ALLOWED_USERS_TO_DELAY_JOBS"], job.queue_name, None, ""
    )

    walltime_min_for_change_hms = duration_to_sql(walltime_min_for_change)
    walltime_max_increase_hms = duration_to_sql(walltime_max_increase)

    # Parse new walltime and convert to seconds
    m = re.match(r"^([-+]?)(\d+)(?::(\d+)(?::(\d+))?)?$", new_walltime)
    if (not m) or (not m.group(2)):  # no match or hour is None
        return (1, 400, "bad request", "syntax error")

    sign, hours, mn, sec = m.groups()

    if not mn:
        mn = 0
    if not sec:
        sec = 0

    new_walltime_seconds = hms_to_duration(hours, mn, sec)
    if sign == "-":
        new_walltime_seconds = -new_walltime_seconds
    elif sign != "+":
        new_walltime_seconds = new_walltime_seconds - moldable.walltime

    # Is walltime change enabled ?
    if not user:
        return (1, 400, "bad request", "anonymous request is not allowed")

    # If force != YES then None
    if force and (force.upper() != "YES"):
        force = None

    # Can extra time delay next jobs ?
    if force and (walltime_users_allowed_to_force != "*"):
        search = re.compile(r"[,\s]+").split(walltime_users_allowed_to_force)
        if user not in (["root", "oar"] + search):
            return (
                3,
                403,
                "forbidden",
                "walltime change for this job is not allowed to be forced",
            )

    # If delay_next_jobs != YES then undef
    if delay_next_jobs and (delay_next_jobs.upper() != "YES"):
        delay_next_jobs = None

    # Can extra time delay next jobs ?
    if delay_next_jobs and (walltime_users_allowed_to_delay_jobs != "*"):
        search = re.compile(r"[,\s]+").split(walltime_users_allowed_to_delay_jobs)
        if user not in (["root", "oar"] + search):
            return (
                3,
                403,
                "forbidden",
                "walltime change for this job is not allowed to delay other jobs",
            )

    # Is job walltime big enough to allow extra time ?
    if moldable.walltime < walltime_min_for_change:
        return (
            3,
            403,
            "forbidden",
            "walltime change is not allowed for a job with walltime < {}".format(
                walltime_min_for_change_hms
            ),
        )

    # For negative extratime, do not allow end time before now
    now = tools.get_date(session)

    suspended = get_job_suspended_sum_duration(session, job_id, now)

    job_types = get_job_types(session, job_id)
    # Arbitrary refuse to reduce container jobs, because we don't want to handle inner jobs which could possibly cross the new boundaries of their container, or should be reduced as well.
    if ("container" in job_types) and (new_walltime_seconds < 0):
        return (
            3,
            403,
            "forbidden",
            "reducing the walltime of a container job is not allowed",
        )

    job_remaining_time = job.start_time + moldable.walltime + suspended - now
    if job_remaining_time < -new_walltime_seconds:
        new_walltime_seconds = -job_remaining_time

    # OAR::IO::lock_table($dbh,['walltime_change']);
    current_walltime_change = get_walltime_change_for_job(session, job_id)
    if current_walltime_change:
        if (
            (walltime_max_increase != -1)
            and (
                current_walltime_change.granted + new_walltime_seconds
                > walltime_max_increase
            )
            and (user not in ("root", "oar"))
        ):
            result = (
                3,
                403,
                "forbidden",
                "request cannot be updated because the walltime cannot increase by more than {}".format(
                    walltime_max_increase_hms
                ),
            )
        else:
            update_walltime_change_request(
                session,
                job_id,
                new_walltime_seconds,
                "YES" if (force and (new_walltime_seconds > 0)) else "NO",
                "YES" if (delay_next_jobs and (new_walltime_seconds > 0)) else "NO",
                None,
                None,
                None,
            )

            result = (
                0,
                202,
                "accepted",
                "walltime change request updated for job {}, it will be handled shortly".format(
                    job_id
                ),
            )
    else:  # New request
        if (
            (walltime_max_increase != -1)
            and (new_walltime_seconds > walltime_max_increase)
            and (user not in ("root", "oar"))
        ):
            result = (
                3,
                403,
                "forbidden",
                "request cannot be accepted because the walltime cannot increase by more than {}".format(
                    walltime_max_increase_hms
                ),
            )
        else:
            add_walltime_change_request(
                session,
                job_id,
                new_walltime_seconds,
                "YES" if (force and (new_walltime_seconds > 0)) else "NO",
                "YES" if (delay_next_jobs and (new_walltime_seconds > 0)) else "NO",
            )
            result = (
                0,
                202,
                "accepted",
                "walltime change request accepted for job {}, it will be handled shortly".format(
                    job_id
                ),
            )

    # OAR::IO::unlock_table($dbh);

    if result[0] == 0:
        tools.notify_almighty("Walltime")

    return result


def add_walltime_change_request(session, job_id, pending, force, delay_next_jobs):
    """Add an extra time request to the database:
    add 1 line to the walltime_change table"""
    walltime_change = WalltimeChange(
        job_id=job_id, pending=pending, force=force, delay_next_jobs=delay_next_jobs
    )
    session.add(walltime_change)
    session.commit()


def update_walltime_change_request(
    session,
    job_id,
    pending,
    force,
    delay_next_jobs,
    granted,
    granted_with_force,
    granted_with_delay_next_jobs,
):
    """Update a walltime change request after processing"""
    walltime_change_update = {
        WalltimeChange.pending: pending,
        # To respect the constaints, `force` and `delay_next_jobs` must be either "YES" or "NO"
        WalltimeChange.force: (force if force else "NO"),
        WalltimeChange.delay_next_jobs: (delay_next_jobs if delay_next_jobs else "NO"),
        WalltimeChange.granted: (granted if granted else 0),
        WalltimeChange.granted_with_force: (
            granted_with_force if granted_with_force else 0
        ),
        WalltimeChange.granted_with_delay_next_jobs: (
            granted_with_delay_next_jobs if granted_with_delay_next_jobs else 0
        ),
    }

    session.query(WalltimeChange).filter(WalltimeChange.job_id == job_id).update(
        walltime_change_update, synchronize_session=False
    )
    session.commit()
