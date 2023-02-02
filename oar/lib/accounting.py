# -*- coding: utf-8 -*-
from sqlalchemy import func, or_

from oar.lib import (
    Accounting,
    AssignedResource,
    Job,
    MoldableJobDescription,
    Resource,
    db,
)

# ACCOUNTING functions
# get_sum_accounting_for_param() -> see Karma.py
# get_sum_accounting_window() -> see Karma.py


def get_accounting_summary(start_time, stop_time, user="", sql_property=""):
    """Get an array of consumptions by users
    params: start date, ending date, optional user"""
    if db.dialect == "sqlite":  # pragma: no cover
        msg = "Get_accounting_summary is not supported with sqlite"
        raise NotImplementedError(msg)

    user_query = "AND accounting_user = '%s'" % (user) if user else ""
    sql_property = "AND ( " + sql_property + " )" if sql_property else ""

    cur = db.session
    res = cur.execute(
        """SELECT accounting_user as user, consumption_type,
    sum(consumption) as seconds,
    min(window_start) as first_window_start, max(window_stop) as last_window_stop
    FROM accounting
    WHERE window_stop > %s AND window_start < %s %s %s
    GROUP BY accounting_user,consumption_type ORDER BY seconds"""
        % (start_time, stop_time, user_query, sql_property)
    )

    results = {}
    for r in res:
        user, consumption_type, consumption, first_window_start, last_window_stop = r
        if user not in results:
            results[user] = {}
        results[user][consumption_type] = int(consumption)
        results[user]["begin"] = first_window_start
        results[user]["end"] = last_window_stop

    return results


def get_accounting_summary_byproject(
    start_time, stop_time, user="", limit="", offset=""
):
    """ "Get an array of consumptions by project for a given user
    params: start date, ending date, user"""

    user_query = ""
    if user:
        user_query = "AND accounting_user = '%s'" % (user)
    limit_offset_query = ""
    if limit:
        limit_offset_query = "LIMIT " + limit
    if offset:
        limit_offset_query = limit_offset_query + " OFFSET " + offset

    cur = db.session
    res = cur.execute(
        """SELECT accounting_user as user, consumption_type,
    sum(consumption) as seconds, accounting_project as project
    FROM accounting
    WHERE
    window_stop > %s AND window_start < %s %s
    GROUP BY accounting_user,project,consumption_type
    ORDER BY project,consumption_type,seconds %s """
        % (start_time, stop_time, user_query, limit_offset_query)
    )

    results = {}
    for r in res:
        user, consumption_type, consumption, project = r
        if project not in results:
            results[project] = {}
        if consumption_type not in results[project]:
            results[project][consumption_type] = {}
        results[project][consumption_type][user] = int(consumption)

    return results


def update_accounting(
    start_time, stop_time, window_size, user, project, queue_name, c_type, nb_resources
):
    """Insert accounting data in table accounting
    # params : start date in second, stop date in second, window size, user, queue, type(ASKED or USED)
    """
    nb_windows = int(start_time / window_size)
    window_start = nb_windows * window_size
    window_stop = window_start + window_size - 1

    consumption = 0
    # Accounting algo
    while stop_time > start_time:
        if stop_time <= window_stop:
            consumption = stop_time - start_time
        else:
            consumption = window_stop - start_time + 1

        consumption = consumption * nb_resources
        add_accounting_row(
            window_start, window_stop, user, project, queue_name, c_type, consumption
        )
        window_start = window_stop + 1
        start_time = window_start
        window_stop += window_size


def add_accounting_row(
    window_start, window_stop, user, project, queue_name, c_type, consumption
):
    # Insert or update one row according to consumption

    # Test if the window exists
    # TODO: Need to be cached (in python process or externaly through Redis by example
    result = (
        db.query(Accounting.consumption)
        .filter(Accounting.user == user)
        .filter(Accounting.project == project)
        .filter(Accounting.consumption_type == c_type)
        .filter(Accounting.queue_name == queue_name)
        .filter(Accounting.window_start == window_start)
        .filter(Accounting.window_stop == window_stop)
        .one_or_none()
    )

    if result:
        consumption = consumption + result[0]
        print(
            "[ACCOUNTING] Update the existing window "
            + str(window_start)
            + " --> "
            + str(window_stop)
            + ", project "
            + project
            + ", user "
            + user
            + ", queue"
            + queue_name
            + ", type "
            + "type with conso = "
            + str(consumption)
            + " s"
        )

        db.query(Accounting).filter(Accounting.user == user).filter(
            Accounting.project == project
        ).filter(Accounting.consumption_type == c_type).filter(
            Accounting.queue_name == queue_name
        ).filter(
            Accounting.window_start == window_start
        ).filter(
            Accounting.window_stop == window_stop
        ).update(
            {Accounting.consumption: consumption}, synchronize_session=False
        )
    else:
        # Create the window
        print(
            "[ACCOUNTING] Create new window "
            + str(window_start)
            + " --> "
            + str(window_stop)
            + ", project "
            + project
            + ", user "
            + user
            + ", queue"
            + queue_name
            + ", type "
            + "type with conso = "
            + str(consumption)
            + " s"
        )

        Accounting.create(
            user=user,
            consumption_type=c_type,
            queue_name=queue_name,
            window_start=window_start,
            window_stop=window_stop,
            consumption=consumption,
            project=project,
        )


def check_accounting_update(window_size):
    """Check jobs that are not treated in accounting table
    params : base, window size"""

    result = (
        db.query(
            Job.start_time,
            Job.stop_time,
            MoldableJobDescription.walltime,
            Job.id,
            Job.user,
            Job.queue_name,
            func.count(AssignedResource.resource_id),
            Job.project,
        )
        .filter(Job.accounted == "NO")
        .filter(or_(Job.state == "Terminated", Job.state == "Error"))
        .filter(Job.stop_time >= Job.start_time)
        .filter(Job.start_time > 1)
        .filter(Job.assigned_moldable_job == MoldableJobDescription.id)
        .filter(AssignedResource.moldable_id == MoldableJobDescription.id)
        .filter(AssignedResource.resource_id == Resource.id)
        .filter(Resource.type == "default")
        .group_by(
            Job.start_time,
            Job.stop_time,
            MoldableJobDescription.walltime,
            Job.id,
            Job.project,
            Job.user,
            Job.queue_name,
        )
        .all()
    )

    for job_accounting_info in result:
        (
            start_time,
            stop_time,
            walltime,
            job_id,
            user,
            queue_name,
            nb_resources,
            project,
        ) = job_accounting_info
        max_stop_time = start_time + walltime
        print("[ACCOUNTING] Treate job " + str(job_id))
        update_accounting(
            start_time,
            stop_time,
            window_size,
            user,
            project,
            queue_name,
            "USED",
            nb_resources,
        )
        update_accounting(
            start_time,
            max_stop_time,
            window_size,
            user,
            project,
            queue_name,
            "ASKED",
            nb_resources,
        )

        db.query(Job).update({Job.accounted: "YES"}, synchronize_session=False)

    db.commit()


def delete_all_from_accounting():
    """Empty the table accounting and update the jobs table."""
    db.query(Accounting).delete(synchronize_session=False)
    db.query(Job).update({Job.accounted: "NO"}, synchronize_session=False)
    db.commit()


def delete_accounting_windows_before(window_stop):
    """Remove windows from accounting."""
    db.query(Accounting).filter(Accounting.window_stop <= window_stop).delete(
        synchronize_session=False
    )
    db.commit()


def get_last_project_karma(user, project, date):
    """Get the last project Karma of user at a given date
    params: user, project, date"""

    cur = db.session
    result = cur.execute(
        """SELECT message FROM jobs
    WHERE job_user='%s' AND message like \'%s\' AND project ='%s' AND start_time < %s
    ORDER BY start_time desc LIMIT 1"""
        % (user, "%Karma%", project, date)
    )
    if result:
        r = result.first()
        if r:
            return r[0]
    return ""
