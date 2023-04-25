# coding: utf-8
from os import environ

import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.meta_sched import meta_schedule
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import (
    AssignedResource,
    FragJob,
    GanttJobsPrediction,
    Job,
    MoldableJobDescription,
    Queue,
    Resource,
)
from oar.lib.queue import get_all_queue_by_priority
from oar.lib.tools import get_date


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
        Queue.create(
            session,
            name="besteffort",
            priority=0,
            scheduler_policy="kamelot",
            state="Active",
        )

        # add some resources
        for i in range(5):
            Resource.create(session, network_address="localhost")

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


def test_db_metasched_simple_1(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    job = minimal_db_initialization.query(Job).one()

    print("job state:", job.state)

    meta_schedule(minimal_db_initialization, config)

    job = minimal_db_initialization.query(Job).one()
    print(job.state)
    assert job.state == "toLaunch"


def test_db_metasched_ar_1(monkeypatch, minimal_db_initialization, setup_config):
    # add one job
    config, _, _ = setup_config
    now = get_date(minimal_db_initialization)
    # sql_now = local_to_sql(now)

    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        reservation="toSchedule",
        start_time=(now + 10),
        info_type="localhost:4242",
    )

    meta_schedule(minimal_db_initialization, config)

    job = minimal_db_initialization.query(Job).one()
    print(job.state, " ", job.reservation)
    print(job.__dict__)
    assert (job.state == "Waiting") and (job.reservation == "Scheduled")


@pytest.fixture(scope="function", autouse=False)
def schedule_some_ar(request, monkeypatch, minimal_db_initialization, setup_config):
    """
    Go back in the past thanks to monkeypatching time, and create an advanced reservation
    """
    config, _, _ = setup_config
    in_the_future = get_date(minimal_db_initialization)
    monkeypatch.setattr(oar.lib.tools, "get_date", lambda x: 100)

    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Waiting",
        reservation="toSchedule",
        start_time=in_the_future,
        info_type="localhost:4242",
    )
    meta_schedule(minimal_db_initialization, config)

    monkeypatch.setattr(oar.lib.tools, "get_date", get_date)


def assign_resources(session, job_id):
    moldable = (
        session.query(MoldableJobDescription)
        .filter(MoldableJobDescription.job_id == job_id)
        .first()
    )

    session.query(Job).filter(Job.id == job_id).update(
        {Job.assigned_moldable_job: moldable.id}, synchronize_session=False
    )
    resources = session.query(Resource).all()
    for r in resources[:4]:
        AssignedResource.create(session, moldable_id=moldable.id, resource_id=r.id)


def test_db_metasched_ar_check_kill_be(
    monkeypatch, schedule_some_ar, minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    environ["USER"] = "root"  # Allow to frag jobs
    now = get_date(minimal_db_initialization)

    # Insert a running best effort jobs with some assigned resources
    job_id = insert_job(
        minimal_db_initialization,
        res=[(500, [("resource_id=5", "")])],
        start_time=now,
        state="Running",
        types=["besteffort"],
        queue_name="besteffort",
    )
    assign_resources(minimal_db_initialization, job_id)

    # Meta scheduler should frag the best effort job
    meta_schedule(minimal_db_initialization, config)

    fragjob = minimal_db_initialization.query(FragJob).first()
    assert fragjob.job_id == job_id


def test_db_metasched_ar_check_kill_be_security_time(
    monkeypatch, minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    environ["USER"] = "root"  # Allow to frag jobs
    config["SCHEDULER_BESTEFFORT_KILL_DURATION_BEFORE_RESERVATION"] = 120

    now = get_date(minimal_db_initialization)
    monkeypatch.setattr(oar.lib.tools, "get_date", lambda x: 100)

    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Waiting",
        reservation="toSchedule",
        start_time=now + 60,
        info_type="localhost:4242",
    )
    meta_schedule(minimal_db_initialization, config)
    monkeypatch.setattr(oar.lib.tools, "get_date", get_date)

    # Insert a running best effort jobs with some assigned resources
    job_id = insert_job(
        minimal_db_initialization,
        res=[(500, [("resource_id=5", "")])],
        start_time=now - 250,
        state="Running",
        types=["besteffort"],
        queue_name="besteffort",
    )
    assign_resources(minimal_db_initialization, job_id)

    # Meta scheduler should frag the best effort job
    meta_schedule(minimal_db_initialization, config)

    fragjob = minimal_db_initialization.query(FragJob).first()
    assert fragjob is not None and fragjob.job_id == job_id


def test_db_metasched_ar_check_no_be_security_time(
    monkeypatch, minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    environ["USER"] = "root"  # Allow to frag jobs
    config["SCHEDULER_BESTEFFORT_KILL_DURATION_BEFORE_RESERVATION"] = 60

    now = get_date(minimal_db_initialization)
    monkeypatch.setattr(oar.lib.tools, "get_date", lambda x: 100)

    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Waiting",
        reservation="toSchedule",
        start_time=now + 120,
        info_type="localhost:4242",
    )
    meta_schedule(minimal_db_initialization, config)
    monkeypatch.setattr(oar.lib.tools, "get_date", get_date)

    # Insert a running best effort jobs with some assigned resources
    job_id = insert_job(
        minimal_db_initialization,
        res=[(500, [("resource_id=5", "")])],
        start_time=now - 250,
        state="Running",
        types=["besteffort"],
        queue_name="besteffort",
    )
    assign_resources(minimal_db_initialization, job_id)

    # Meta scheduler should frag the best effort job
    meta_schedule(minimal_db_initialization, config)

    fragjob = minimal_db_initialization.query(FragJob).first()
    assert fragjob is None


def test_call_external_scheduler_fails(
    monkeypatch, minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    # Ensure that we don't find an external scheduler
    environ["OARDIR"] = "/dev/null"
    meta_schedule(minimal_db_initialization, config, mode="external")

    findQueue = False
    for queue in get_all_queue_by_priority(minimal_db_initialization):
        if queue.name == "default":
            findQueue = True
            assert queue.state == "notActive"

    # Check that the default queue has been found an tested
    assert findQueue


def test_db_metasched_be_released_for_job(
    monkeypatch, minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    environ["USER"] = "root"  # Allow to frag jobs

    now = get_date(minimal_db_initialization)
    monkeypatch.setattr(oar.lib.tools, "get_date", lambda x: 100)

    job_id = insert_job(
        minimal_db_initialization,
        res=[(500, [("resource_id=5", "")])],
        start_time=now - 250,
        state="Running",
        types=["besteffort"],
        queue_name="besteffort",
    )
    assign_resources(minimal_db_initialization, job_id)

    meta_schedule(minimal_db_initialization, config)
    monkeypatch.setattr(oar.lib.tools, "get_date", get_date)

    # Insert a running best effort jobs with some assigned resources
    insert_job(
        minimal_db_initialization,
        res=[(500, [("resource_id=4", "")])],
        queue_name="default",
    )

    # Meta scheduler should frag the best effort job
    meta_schedule(minimal_db_initialization, config)

    fragjob = minimal_db_initialization.query(FragJob).first()
    assert fragjob.job_id == job_id


def test_db_metasched_ar_2(monkeypatch, minimal_db_initialization, setup_config):
    """
    Test multiple AR reservation in the same metaschedule.
    The first and third job should be scheduled, wheres
    """
    config, _, _ = setup_config
    in_the_future = get_date(minimal_db_initialization)

    # Mock so the first metaschedule is in the "past"
    monkeypatch.setattr(oar.lib.tools, "get_date", lambda x: 100)
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Waiting",
        reservation="toSchedule",
        start_time=in_the_future,
        info_type="localhost:4242",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        properties="",
        state="Waiting",
        reservation="toSchedule",
        start_time=in_the_future,
        info_type="localhost:4243",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=1", "")])],
        properties="",
        state="Waiting",
        reservation="toSchedule",
        start_time=in_the_future,
        info_type="localhost:4243",
    )

    meta_schedule(minimal_db_initialization, config)
    assert len(minimal_db_initialization.query(GanttJobsPrediction).all()) == 2

    # Restore the get date function
    monkeypatch.setattr(oar.lib.tools, "get_date", get_date)
