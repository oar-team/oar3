# coding: utf-8
import pytest
from procset import ProcSet
from sqlalchemy import event
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.platform import Platform
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import (
    check_end_of_job,
    get_data_jobs,
    insert_job,
    job_message,
    save_assigns,
)
from oar.lib.models import EventLog, Job, Resource

NB_JOBS = 5


@pytest.fixture(scope="function", autouse=False)
def minimal_db_initialization(request, setup_config):
    _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)


@pytest.mark.parametrize(
    "error, event_type",
    [
        (0, "SWITCH_INTO_TERMINATE_STATE"),
        (1, "PROLOGUE_ERROR"),
        (2, "EPILOGUE_ERROR"),
        (3, "SWITCH_INTO_ERROR_STATE"),
        (5, "CANNOT_WRITE_NODE_FILE"),
        (6, "CANNOT_WRITE_PID_FILE"),
        (7, "USER_SHELL"),
        (8, "CANNOT_CREATE_TMP_DIRECTORY"),
        (10, "SWITCH_INTO_ERROR_STATE"),
        (20, "SWITCH_INTO_ERROR_STATE"),
        (12, "SWITCH_INTO_ERROR_STATE"),
        (22, "SWITCH_INTO_ERROR_STATE"),
        (30, "SSH_TRANSFER_TIMEOUT"),
        (31, "BAD_HASHTABLE_DUMP"),
        (33, "SWITCH_INTO_TERMINATE_STATE"),
        (34, "SWITCH_INTO_TERMINATE_STATE"),
        (50, "LAUNCHING_OAREXEC_TIMEOUT"),
        (40, "SWITCH_INTO_TERMINATE_STATE"),
        (42, "SWITCH_INTO_TERMINATE_STATE"),
        (41, "SWITCH_INTO_TERMINATE_STATE"),
        (12345, "EXIT_VALUE_OAREXEC"),
    ],
)
def test_check_end_of_job(error, event_type, minimal_db_initialization, setup_config):
    config, _ = setup_config

    config["OAREXEC_DIRECTORY"] = "/tmp/foo"
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Launching",
    )
    check_end_of_job(
        minimal_db_initialization,
        config,
        job_id,
        0,
        error,
        ["node1"],
        "toto",
        "/home/toto",
        None,
    )
    event = minimal_db_initialization.query(EventLog).first()
    assert event.type == event_type


def test_get_data_jobs_moldable(monkeypatch, minimal_db_initialization, setup_config):
    config, _ = setup_config
    # Create a moldable job
    test_jobs = []
    job_id = insert_job(
        minimal_db_initialization,
        res=[
            (20, [("resource_id=4/cpu=2", "")]),
            (20, [("resource_id=4/cpu=2", "")]),
        ],
    )
    test_jobs.append((job_id, 2))
    job_id = insert_job(
        minimal_db_initialization,
        res=[
            (20, [("resource_id=4/cpu=2", "")]),
        ],
    )
    test_jobs.append((job_id, 1))

    job_id = insert_job(
        minimal_db_initialization,
        res=[
            (20, [("resource_id=4/cpu=2", "")]),
            (70, [("resource_id=1/cpu=3", "")]),
            (120, [("resource_id=1/cpu=1", "")]),
        ],
    )
    test_jobs.append((job_id, 3))

    plt = Platform()
    jobs = plt.get_waiting_jobs("default", session=minimal_db_initialization)
    # Get the data
    get_data_jobs(
        minimal_db_initialization,
        jobs[0],
        jobs[1],
        plt.resource_set(minimal_db_initialization, config),
        5,
    )

    for job_and_nb_moldable in test_jobs:
        test_job_id = job_and_nb_moldable[0]
        test_nb_mold = job_and_nb_moldable[1]
        # Assert that the jobs has two moldable
        assert len(jobs[0][test_job_id].mld_res_rqts) == test_nb_mold


def test_job_message(minimal_db_initialization):
    session = minimal_db_initialization

    # Job avec job_name
    job_id_with_name = insert_job(
        session,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
        job_user="Toto",
        job_name="Titi",
    )

    # Job sans job_name
    job_id_without_name = insert_job(
        session,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
        job_user="Toto",
    )

    job_with_name = session.query(Job).filter(Job.id == job_id_with_name).one()
    job_without_name = session.query(Job).filter(Job.id == job_id_without_name).one()

    result_with_name = job_message(session, job_with_name)
    assert "N=Titi" in result_with_name

    result_without_name = job_message(session, job_without_name)
    assert "N=" not in result_without_name


def _make_schedulable_job(session, karma=None):
    """Insert a job and decorate it with the attributes save_assigns expects.

    ``start_time`` is left at its DB default (0) and ``moldable_id``,
    ``res_set``, ``walltime``, ``karma`` are set as plain (non-column)
    attributes, exactly like the scheduler does on its in-memory jobs.
    """
    job_id, moldable_ids = insert_job(
        session,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Waiting",
        return_moldable=True,
    )
    job = session.query(Job).filter(Job.id == job_id).one()
    job.moldable_id = moldable_ids[0]
    job.res_set = ProcSet(0)  # internal (ordinal) resource id
    job.walltime = 60
    if karma is not None:
        job.karma = karma
    return job


def _count_jobs_updates(engine):
    """Return a (list, callback) pair recording UPDATE statements on jobs."""
    updates = []

    def before_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):
        stmt = statement.strip()
        if stmt.upper().startswith("UPDATE") and "jobs" in stmt:
            updates.append(stmt)

    event.listen(engine, "before_cursor_execute", before_cursor_execute)
    return updates, before_cursor_execute


def test_job_message_does_not_persist_last_karma(minimal_db_initialization):
    """job_message must not write Job.last_karma nor commit on its own."""
    session = minimal_db_initialization
    job = _make_schedulable_job(session, karma=42.5)
    job_id = job.id

    commits = []
    event.listen(session, "after_commit", lambda s: commits.append(1))

    message = job_message(session, job)

    assert "(Karma=42.5)" in message
    # The per-job commit path (set_job_last_karma) is gone.
    assert commits == []
    session.expire_all()
    assert session.query(Job.last_karma).filter(Job.id == job_id).scalar() is None


def test_save_assigns_batches_last_karma(minimal_db_initialization, setup_config):
    """save_assigns writes every last_karma in a single bulk UPDATE."""
    session = minimal_db_initialization
    config, engine = setup_config

    # Resources must exist so resource_set.rid_o2i maps ordinal -> real id.
    for _ in range(3):
        Resource.create(session, network_address="localhost")
    resource_set = Platform().resource_set(session, config)

    karmas = [1.5, 2.5, 3.5]
    jobs = [_make_schedulable_job(session, karma=k) for k in karmas]
    # A job without a karma attribute must not get a last_karma write.
    job_without_karma = _make_schedulable_job(session, karma=None)
    jobs.append(job_without_karma)

    updates, callback = _count_jobs_updates(engine)
    commits = []
    event.listen(session, "after_commit", lambda s: commits.append(1))
    try:
        save_assigns(session, jobs, resource_set)
    finally:
        event.remove(engine, "before_cursor_execute", callback)

    # One bulk UPDATE for messages + one bulk UPDATE for last_karma, and a
    # single commit for the whole batch (not one per job).
    assert len(updates) == 2
    assert len(commits) == 1

    session.expire_all()
    for job, karma in zip(jobs[: len(karmas)], karmas):
        assert session.query(Job.last_karma).filter(Job.id == job.id).scalar() == karma
    assert (
        session.query(Job.last_karma).filter(Job.id == job_without_karma.id).scalar()
        is None
    )
