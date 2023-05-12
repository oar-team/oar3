# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.platform import Platform
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import check_end_of_job, get_data_jobs, insert_job
from oar.lib.models import EventLog


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
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
    config, _, _ = setup_config

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
    config, _, _ = setup_config
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
