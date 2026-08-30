# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.platform import Platform
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import (
    check_end_of_job,
    get_data_jobs,
    insert_job,
    job_message,
    get_custom_notification_message,
)
from oar.lib.models import EventLog, Job

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


def test_get_custom_notification_message(
    minimal_db_initialization, setup_config, tmp_path
):
    config, _ = setup_config
    session = minimal_db_initialization

    # Create a test job
    job_id = insert_job(
        session,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
        job_user="Toto",
    )
    job = session.query(Job).filter(Job.id == job_id).one()

    # Case 1: No template configured -> Must return None
    assert (
        get_custom_notification_message(config, "RUNNING", job, session, job_id) is None
    )

    # Case 2: Valid template -> Must replace the tags
    valid_tpl = tmp_path / "valid.txt"
    valid_tpl.write_text("Hello {user}, Job {id} is {state}")
    config["MAIL_TEMPLATE_RUNNING"] = str(valid_tpl)

    res = get_custom_notification_message(config, "RUNNING", job, session, job_id)
    assert res is not None
    assert "Hello Toto" in res
    assert f"Job {job_id} is RUNNING" in res

    # Case 3: Invalid template -> Must return None without crashing
    bad_tpl = tmp_path / "bad.txt"
    bad_tpl.write_text("Crash {unknown_variable}")
    config["MAIL_TEMPLATE_ERROR"] = str(bad_tpl)

    assert (
        get_custom_notification_message(config, "ERROR", job, session, job_id) is None
    )
