# coding: utf-8
"""Integration test: running jobs must influence fairshare ordering.

Mirrors the fixtures of ``tests/kao/test_db_fairshare.py`` and drives the
scheduler through ``Platform`` + ``schedule_cycle``.  A single running job
belonging to ``zozo0`` dominates the fairsharing window.  When
``SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS`` is enabled, the next job of
``zozo0`` must be penalized (not scheduled first); when disabled, ``zozo0``
keeps the lowest accounting and is scheduled first.
"""

import time

import pytest
from sqlalchemy import delete, insert
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import (
    Accounting,
    GanttJobsPrediction,
    Job,
    MoldableJobDescription,
    Resource,
)

from ..helpers import insert_running_jobs


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        yield session


@pytest.fixture(scope="module", autouse=True)
def oar_conf(request, setup_config):
    config, _ = setup_config
    old_priority = config.get("JOB_PRIORITY")
    old_include_running = config.get("SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS")
    config["JOB_PRIORITY"] = "FAIRSHARE"
    config["SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS"] = "yes"

    @request.addfinalizer
    def remove_fairsharing():
        # Restore the session-scoped config so following test modules are not
        # polluted by this module's scenario runs.
        if old_priority is None:
            config.pop("JOB_PRIORITY", None)
        else:
            config["JOB_PRIORITY"] = old_priority
        if old_include_running is None:
            config.pop("SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS", None)
        else:
            config["SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS"] = old_include_running


def del_accounting(session):
    session.execute(delete(Accounting))
    session.commit()


def set_accounting(session, accountings, consumption_type):
    ins_accountings = []
    for a in accountings:
        w_start, w_stop, proj, user, queue, consumption = a
        ins_accountings.append(
            {
                "window_start": w_start,
                "window_stop": w_stop,
                "accounting_project": proj,
                "user": user,
                "queue_name": queue,
                "consumption_type": consumption_type,
                "consumption": consumption,
            }
        )

    session.execute(insert(Accounting), ins_accountings)
    session.commit()


def generate_accountings(
    session, nb_users=5, t_window=24 * 36000, queue="default", project="default"
):
    del_accounting(session)

    nb_accounts = 5

    now = time.time()
    offset = now - t_window * nb_accounts

    accountings_a = []
    accountings_u = []
    for u in range(nb_users):
        user = "zozo" + str(u)

        for i in range(nb_accounts):
            w_start = t_window * (i + 1) + offset
            w_stop = t_window * (i + 1) + t_window / 10 + offset
            consumption = 1000 * (1 + u)

            accountings_a.append(
                (w_start, w_stop, project, user, queue, consumption + 1000)
            )
            accountings_u.append((w_start, w_stop, project, user, queue, consumption))

    set_accounting(session, accountings_a, "ASKED")
    set_accounting(session, accountings_u, "USED")


def _scheduled_users(session, jid_2_user):
    """Return the users of the scheduled jobs, in scheduling order."""
    req = (
        session.query(GanttJobsPrediction)
        .order_by(GanttJobsPrediction.start_time)
        .all()
    )
    ordered_users = []
    for pred in req:
        jid = (
            session.query(MoldableJobDescription.job_id)
            .filter(MoldableJobDescription.id == pred.moldable_id)
            .scalar()
        )
        if jid in jid_2_user:
            ordered_users.append(jid_2_user[jid])
    return ordered_users


def _run_scenario(session, config, include_running):
    config["SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS"] = include_running
    window_size = int(config["SCHEDULER_FAIRSHARING_WINDOW_SIZE"])
    now = int(time.time())

    generate_accountings(session)

    for _ in range(5):
        Resource.create(session, network_address="localhost")

    # One running job owned by zozo0, started at the beginning of the window,
    # with a long walltime so that its consumption dominates the window.
    running_jids = insert_running_jobs(
        session,
        nb_jobs=1,
        user="zozo0",
        project="default",
        walltime=10 * window_size,
    )
    session.query(Job).filter(Job.id == running_jids[0]).update(
        {Job.start_time: now - window_size}, synchronize_session=False
    )
    session.commit()

    jid_2_user = {}
    for u in range(5):
        user = "zozo" + str(u)
        jid = insert_job(
            session,
            job_user=user,
            res=[(60, [("resource_id=4", "")])],
            properties="",
        )
        jid_2_user[jid] = user

    plt = Platform()
    schedule_cycle(session, config, plt, now)

    return _scheduled_users(session, jid_2_user)


def test_running_job_owner_is_penalized(minimal_db_initialization, setup_config):
    config, _ = setup_config
    ordered_users = _run_scenario(
        minimal_db_initialization, config, include_running="yes"
    )

    assert len(ordered_users) == 5
    # zozo0 dominates the running consumption, so its next job must not be
    # scheduled first.
    assert ordered_users[0] != "zozo0"


def test_running_job_owner_first_when_excluded(minimal_db_initialization, setup_config):
    config, _ = setup_config
    ordered_users = _run_scenario(
        minimal_db_initialization, config, include_running="no"
    )

    assert len(ordered_users) == 5
    # Without running consumption, zozo0 has the lowest accounting and is
    # scheduled first.
    assert ordered_users[0] == "zozo0"
