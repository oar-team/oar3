# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.lib.database import ephemeral_session
from oar.lib.globals import get_logger, init_oar
from oar.lib.job_handling import insert_job
from oar.lib.models import (
    AssignedResource,
    EventLog,
    FragJob,
    Job,
    MoldableJobDescription,
    Resource,
    ResourceLog,
)
from oar.modules.sarko import Sarko

_, _, log = init_oar(no_db=True)

logger = get_logger("test_sarko")

fake_date = 0


def set_fake_date(date):
    global fake_date
    fake_date = date


def fake_get_date(session):
    return fake_date


def fake_signal_oarexec(host, job_id, signal_name, detach, openssh_cmd):
    return ""


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources
        for i in range(5):
            Resource.create(session, network_address="localhost" + str(i))
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "get_date", fake_get_date)
    monkeypatch.setattr(oar.lib.tools, "signal_oarexec", fake_signal_oarexec)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)


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
    print(resources)
    for r in resources[:4]:
        AssignedResource.create(session, moldable_id=job_id, resource_id=r.id)


def test_sarko_void(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    # print("yop")
    # for j in minimal_db_initializationquery(Job).all():
    #    print(j)
    sarko = Sarko(config, logger)
    sarko.run(minimal_db_initialization)
    print(sarko.guilty_found)
    assert sarko.guilty_found == 0


def test_sarko_job_walltime_reached(minimal_db_initialization, setup_config):
    """date > (start_time + max_time):"""
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
    )
    assign_resources(minimal_db_initialization, job_id)

    set_fake_date(100)

    sarko = Sarko(config, logger)
    sarko.run(
        minimal_db_initialization,
    )

    # Reset date
    set_fake_date(0)

    print(sarko.guilty_found)
    assert sarko.guilty_found == 1


def test_sarko_job_to_checkpoint(minimal_db_initialization, setup_config):
    """(date >= (start_time + max_time - job.checkpoint))"""
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
        checkpoint=30,
    )
    assign_resources(minimal_db_initialization, job_id)

    set_fake_date(45)  # > 0+60 - 30
    sarko = Sarko(config, logger)
    sarko.run(
        minimal_db_initialization,
    )
    # Reset date
    set_fake_date(0)

    event = (
        minimal_db_initialization.query(EventLog)
        .filter(EventLog.type == "CHECKPOINT_SUCCESSFULL")
        .first()
    )

    print(sarko.guilty_found)
    assert sarko.guilty_found == 0
    assert event.job_id == job_id


def test_sarko_timer_armed_job_terminated(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Terminated",
    )
    assign_resources(minimal_db_initialization, job_id)

    FragJob.create(minimal_db_initialization, job_id=job_id, state="TIMER_ARMED")

    sarko = Sarko(config, logger)
    sarko.run(
        minimal_db_initialization,
    )

    fragjob = (
        minimal_db_initialization.query(FragJob)
        .filter(FragJob.state == "FRAGGED")
        .first()
    )
    assert fragjob.job_id == job_id
    assert sarko.guilty_found == 0


def test_sarko_timer_armed_job_running(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
    )
    assign_resources(minimal_db_initialization, job_id)

    FragJob.create(minimal_db_initialization, job_id=job_id, state="TIMER_ARMED")

    sarko = Sarko(config, logger)
    sarko.run(
        minimal_db_initialization,
    )

    assert sarko.guilty_found == 0


def test_sarko_timer_armed_job_refrag(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
    )
    assign_resources(minimal_db_initialization, job_id)

    FragJob.create(minimal_db_initialization, job_id=job_id, state="TIMER_ARMED")

    set_fake_date(100)
    sarko = Sarko(config, logger)
    sarko.run(
        minimal_db_initialization,
    )

    set_fake_date(0)
    fragjob = (
        minimal_db_initialization.query(FragJob).filter(FragJob.state == "LEON").first()
    )
    assert fragjob.job_id == job_id
    assert sarko.guilty_found == 1


def test_sarko_timer_armed_job_exterminate(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
    )
    assign_resources(minimal_db_initialization, job_id)

    FragJob.create(minimal_db_initialization, job_id=job_id, state="TIMER_ARMED")

    set_fake_date(400)
    sarko = Sarko(config, logger)
    sarko.run(
        minimal_db_initialization,
    )

    set_fake_date(0)
    fragjob = (
        minimal_db_initialization.query(FragJob)
        .filter(FragJob.state == "LEON_EXTERMINATE")
        .first()
    )
    assert fragjob.job_id == job_id
    assert sarko.guilty_found == 1


def test_sarko_dead_switch_time_none(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    config["DEAD_SWITCH_TIME"] = "100"
    sarko = Sarko(config, logger)
    sarko.run(
        minimal_db_initialization,
    )
    assert sarko.guilty_found == 0


def test_sarko_dead_switch_time_one(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    config["DEAD_SWITCH_TIME"] = "100"

    resources = minimal_db_initialization.query(Resource).all()
    r_id = resources[0].id

    ResourceLog.create(
        minimal_db_initialization, resource_id=r_id, attribute="state", date_start=50
    )

    set_fake_date(400)
    sarko = Sarko(config, logger)
    sarko.run(
        minimal_db_initialization,
    )
    set_fake_date(0)

    resource = (
        minimal_db_initialization.query(Resource).filter(Resource.id == r_id).first()
    )

    assert resource.next_state == "Dead"
    assert sarko.guilty_found == 0


def test_sarko_expired_resources(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    resources = minimal_db_initialization.query(Resource).all()
    r_id = resources[0].id

    minimal_db_initialization.query(Resource).filter(Resource.id == r_id).update(
        {Resource.expiry_date: 50, Resource.desktop_computing: "YES"},
        synchronize_session=False,
    )

    set_fake_date(100)
    sarko = Sarko(config, logger)
    sarko.run(
        minimal_db_initialization,
    )
    set_fake_date(0)

    resource = (
        minimal_db_initialization.query(Resource).filter(Resource.id == r_id).first()
    )

    assert resource.next_state == "Suspected"
    assert sarko.guilty_found == 0
