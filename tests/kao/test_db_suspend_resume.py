# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.meta_sched import meta_schedule
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job, set_job_state
from oar.lib.models import Job, Queue, Resource


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:

        Queue.create(
            session,
            name="default",
            priority=3,
            scheduler_policy="kamelot",
            state="Active",
        )

        # add some resources
        for i in range(5):
            Resource.create(session, network_address="localhost" + str(int(i / 2)))

        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "notify_tcp_socket", lambda addr, port, msg: len(msg)
    )
    monkeypatch.setattr(
        oar.lib.tools, "notify_user", lambda job, state, msg: len(state + msg)
    )
    monkeypatch.setattr(oar.lib.tools, "notify_bipbip_commander", lambda json_msg: True)


@pytest.fixture(scope="function")
def config_suspend_resume(request, setup_config):
    config, _, _ = setup_config
    config["JUST_BEFORE_RESUME_EXEC_FILE"] = "true"
    config["SUSPEND_RESUME_SCRIPT_TIMEOUT"] = "1"

    yield config

    def teardown():
        del config["JUST_BEFORE_RESUME_EXEC_FILE"]
        del config["SUSPEND_RESUME_SCRIPT_TIMEOUT"]

    request.addfinalizer(teardown)


def test_suspend_resume_1(
    monkeypatch, minimal_db_initialization, config_suspend_resume
):
    # now = get_date()
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    meta_schedule(minimal_db_initialization, config_suspend_resume, "internal")
    job = minimal_db_initialization.query(Job).one()
    print(job.state)
    set_job_state(minimal_db_initialization, job.id, "Resuming")
    job = minimal_db_initialization.query(Job).one()
    print(job.state)
    meta_schedule(minimal_db_initialization, config_suspend_resume, "internal")
    assert job.state == "Resuming"
    # assert(True)


def test_suspend_resume_2(
    monkeypatch, minimal_db_initialization, config_suspend_resume
):
    config = config_suspend_resume
    config["JUST_BEFORE_RESUME_EXEC_FILE"] = "sleep 2"
    # now = get_date()
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    meta_schedule(minimal_db_initialization, config, "internal")
    job = minimal_db_initialization.query(Job).one()
    print(job.state)
    set_job_state(minimal_db_initialization, job.id, "Resuming")
    job = minimal_db_initialization.query(Job).one()
    print(job.state)
    meta_schedule(minimal_db_initialization, config, "internal")
    assert job.state == "Resuming"
