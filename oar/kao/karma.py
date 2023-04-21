# coding: utf-8

import re

from sqlalchemy import func

from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from oar.lib.models import Accounting

_, _, log = init_oar()

# Log category
logger = get_logger(log, "oar.kao.karma")


# convert perl hash 2 dict
def perl_hash_2_dict(str):
    d = {}
    # remove space and curly bracket
    str = re.sub(r"{|}|\s", "", str)
    for pair in re.split(",", str):
        kv = re.split("=>", pair)
        d[kv[0]] = kv[1]
    return d


def get_sum_accounting_window(session, queues, window_start, window_stop):
    req = (
        session.query(Accounting.consumption_type, func.sum(Accounting.consumption))
        .filter(Accounting.queue_name.in_(tuple(queues)))
        .filter(Accounting.window_start >= window_start)
        .filter(Accounting.window_stop < window_stop)
        .group_by(Accounting.consumption_type)
        .all()
    )

    karma_sum_time_asked = 1
    karma_sum_time_used = 1

    for consumption_type, total_consumption in req:
        if consumption_type == "ASKED":
            karma_sum_time_asked = float(total_consumption)
        elif consumption_type == "USED":
            karma_sum_time_used = float(total_consumption)

    return (karma_sum_time_asked, karma_sum_time_used)


# and karma_projects_asked, karma_projects_used =
# Iolib.get_sum_accounting_for_param dbh queue "accounting_project"
# window_start window_stop


def get_sum_accounting_by_project(session, queues, window_start, window_stop):
    req = (
        session.query(
            Accounting.project,
            Accounting.consumption_type,
            func.sum(Accounting.consumption),
        )
        .filter(Accounting.queue_name.in_(tuple(queues)))
        .filter(Accounting.window_start >= window_start)
        .filter(Accounting.window_stop < window_stop)
        .group_by(Accounting.project, Accounting.consumption_type)
        .all()
    )

    karma_used = {}
    karma_asked = {}

    for project, consumption_type, total_consumption in req:
        # print "project, consumption_type, total_consumption: ", project,
        # consumption_type, total_consumption
        if consumption_type == "ASKED":
            karma_asked[project] = float(total_consumption)
        elif consumption_type == "USED":
            karma_used[project] = float(total_consumption)

    return (karma_asked, karma_used)


def get_sum_accounting_by_user(session, queues, window_start, window_stop):
    # print " window_start, window_stop", window_start, window_stop
    req = (
        session.query(
            Accounting.user,
            Accounting.consumption_type,
            func.sum(Accounting.consumption),
        )
        .filter(Accounting.queue_name.in_(tuple(queues)))
        .filter(Accounting.window_start >= window_start)
        .filter(Accounting.window_stop <= window_stop)
        .group_by(Accounting.user, Accounting.consumption_type)
        .all()
    )

    karma_used = {}
    karma_asked = {}

    for user, consumption_type, total_consumption in req:
        # print "user, consumption_type, total_consumption:", user,
        # consumption_type, total_consumption
        if consumption_type == "ASKED":
            karma_asked[user] = float(total_consumption)
        elif consumption_type == "USED":
            karma_used[user] = float(total_consumption)

    return (karma_asked, karma_used)


#
# Evaluate Karma value for each job
#
def evaluate_jobs_karma(config, queues, now, jids, jobs, plt):
    # if "SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER" in config:
    #    fairsharing_nb_job_limit = config["SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER"]
    # TODO NOT UDSED
    # fairsharing_nb_job_limit = 100000

    karma_window_size = config["SCHEDULER_FAIRSHARING_WINDOW_SIZE"]

    # get fairsharing config if any
    karma_proj_targets = perl_hash_2_dict(
        config["SCHEDULER_FAIRSHARING_PROJECT_TARGETS"]
    )
    karma_user_targets = perl_hash_2_dict(config["SCHEDULER_FAIRSHARING_USER_TARGETS"])
    karma_coeff_proj_consumption = float(config["SCHEDULER_FAIRSHARING_COEF_PROJECT"])
    karma_coeff_user_consumption = float(config["SCHEDULER_FAIRSHARING_COEF_USER"])
    karma_coeff_user_asked_consumption = float(
        config["SCHEDULER_FAIRSHARING_COEF_USER_ASK"]
    )

    #
    # Retrieve karma part value from past
    #

    window_start = now - karma_window_size
    window_stop = now

    karma_sum_time_asked, karma_sum_time_used = plt.get_sum_accounting_window(
        queues, window_start, window_stop
    )
    karma_projects_asked, karma_projects_used = plt.get_sum_accounting_by_project(
        queues, window_start, window_stop
    )
    karma_users_asked, karma_users_used = plt.get_sum_accounting_by_user(
        queues, window_start, window_stop
    )
    #
    # Compute actual karma for each job
    #

    for job in jobs.values():
        if job.project in karma_projects_used:
            karma_proj_used_j = karma_projects_used[job.project]
        else:
            karma_proj_used_j = 0.0

        if job.user in karma_users_used:
            karma_user_used_j = karma_users_used[job.user]
        else:
            karma_user_used_j = 0.0

        if job.user in karma_users_asked:
            karma_user_asked_j = karma_users_asked[job.user]
        else:
            karma_user_asked_j = 0.0

        if job.project in karma_proj_targets:
            karma_proj_target = float(karma_proj_targets[job.project])
        else:
            karma_proj_target = 0.0

        if job.user in karma_user_targets:
            karma_user_target = karma_user_targets[job.user] / 100.0
        else:
            karma_user_target = 0.0

        # x1 = karma_coeff_proj_consumption * ((karma_proj_used_j / karma_sum_time_used) - (karma_proj_target / 100.0))
        # x2 = karma_coeff_user_consumption * ((karma_user_used_j / karma_sum_time_used) - (karma_user_target / 100.0))
        # x3 = karma_coeff_user_asked_consumption * ((karma_user_asked_j / karma_sum_time_asked) - (karma_user_target / 100.0))
        # print "yopypop", x1, x2, x3
        projet = karma_coeff_proj_consumption * (
            (karma_proj_used_j / karma_sum_time_used) - (karma_proj_target / 100.0)
        )
        user = karma_coeff_user_consumption * (
            (karma_user_used_j / karma_sum_time_used) - (karma_user_target / 100.0)
        )
        user_ask = karma_coeff_user_asked_consumption * (
            (karma_user_asked_j / karma_sum_time_asked) - (karma_user_target / 100.0)
        )
        job.karma = projet + user + user_ask


def karma_jobs_sorting(config, queues, now, jids, jobs, plt):
    evaluate_jobs_karma(config, queues, now, jids, jobs, plt)
    #
    # Sort jobs accordingly to karma value (fairsharing)
    #
    karma_ordered_jids = sorted(jids, key=lambda jid: jobs[jid].karma)
    # print karma_ordered_jids
    return karma_ordered_jids
