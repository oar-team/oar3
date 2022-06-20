# coding: utf-8
import pytest

import oar.lib.tools  # for monkeypatching
from oar.lib import (
    AssignedResource,
    EventLog,
    FragJob,
    Job,
    Resource,
    ResourceLog,
    config,
    db,
)
from oar.lib.job_handling import insert_job
from oar.modules.sarko import Sarko

fake_date = 0


def set_fake_date(date):
    global fake_date
    fake_date = date


def fake_get_date():
    return fake_date


def fake_signal_oarexec(host, job_id, signal_name, detach, openssh_cmd):
    return ""


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            Resource.create(network_address="localhost" + str(i))
        yield


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "get_date", fake_get_date)
    monkeypatch.setattr(oar.lib.tools, "signal_oarexec", fake_signal_oarexec)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)


def assign_resources(job_id):
    db.query(Job).filter(Job.id == job_id).update(
        {Job.assigned_moldable_job: job_id}, synchronize_session=False
    )
    resources = db.query(Resource).all()
    print(resources)
    for r in resources[:4]:
        AssignedResource.create(moldable_id=job_id, resource_id=r.id)


def test_sarko_void():
    # print("yop")
    # for j in db.query(Job).all():
    #    print(j)
    sarko = Sarko()
    sarko.run()
    print(sarko.guilty_found)
    assert sarko.guilty_found == 0


def test_sarko_job_walltime_reached():
    """date > (start_time + max_time):"""
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", state="Running"
    )
    assign_resources(job_id)

    set_fake_date(100)

    sarko = Sarko()
    sarko.run()

    # Reset date
    set_fake_date(0)

    print(sarko.guilty_found)
    assert sarko.guilty_found == 1


def test_sarko_job_to_checkpoint():
    """(date >= (start_time + max_time - job.checkpoint))"""
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
        checkpoint=30,
    )
    assign_resources(job_id)

    set_fake_date(45)  # > 0+60 - 30
    sarko = Sarko()
    sarko.run()
    # Reset date
    set_fake_date(0)

    event = db.query(EventLog).filter(EventLog.type == "CHECKPOINT_SUCCESSFULL").first()

    print(sarko.guilty_found)
    assert sarko.guilty_found == 0
    assert event.job_id == job_id


def test_sarko_timer_armed_job_terminated():
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", state="Terminated"
    )
    assign_resources(job_id)

    FragJob.create(job_id=job_id, state="TIMER_ARMED")

    sarko = Sarko()
    sarko.run()

    fragjob = db.query(FragJob).filter(FragJob.state == "FRAGGED").first()
    assert fragjob.job_id == job_id
    assert sarko.guilty_found == 0


def test_sarko_timer_armed_job_running():
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", state="Running"
    )
    assign_resources(job_id)

    FragJob.create(job_id=job_id, state="TIMER_ARMED")

    sarko = Sarko()
    sarko.run()
    assert sarko.guilty_found == 0


def test_sarko_timer_armed_job_refrag():
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", state="Running"
    )
    assign_resources(job_id)

    FragJob.create(job_id=job_id, state="TIMER_ARMED")

    set_fake_date(100)
    sarko = Sarko()
    sarko.run()
    set_fake_date(0)
    fragjob = db.query(FragJob).filter(FragJob.state == "LEON").first()
    assert fragjob.job_id == job_id
    assert sarko.guilty_found == 1


def test_sarko_timer_armed_job_exterminate():
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", state="Running"
    )
    assign_resources(job_id)

    FragJob.create(job_id=job_id, state="TIMER_ARMED")

    set_fake_date(400)
    sarko = Sarko()
    sarko.run()
    set_fake_date(0)
    fragjob = db.query(FragJob).filter(FragJob.state == "LEON_EXTERMINATE").first()
    assert fragjob.job_id == job_id
    assert sarko.guilty_found == 1


def test_sarko_dead_switch_time_none():
    config["DEAD_SWITCH_TIME"] = "100"
    sarko = Sarko()
    sarko.run()
    assert sarko.guilty_found == 0


def test_sarko_dead_switch_time_one():
    config["DEAD_SWITCH_TIME"] = "100"

    resources = db.query(Resource).all()
    r_id = resources[0].id

    ResourceLog.create(resource_id=r_id, attribute="state", date_start=50)

    set_fake_date(400)
    sarko = Sarko()
    sarko.run()
    set_fake_date(0)

    resource = db.query(Resource).filter(Resource.id == r_id).first()

    assert resource.next_state == "Dead"
    assert sarko.guilty_found == 0


def test_sarko_expired_resources():
    resources = db.query(Resource).all()
    r_id = resources[0].id

    db.query(Resource).filter(Resource.id == r_id).update(
        {Resource.expiry_date: 50, Resource.desktop_computing: "YES"},
        synchronize_session=False,
    )

    set_fake_date(100)
    sarko = Sarko()
    sarko.run()
    set_fake_date(0)

    resource = db.query(Resource).filter(Resource.id == r_id).first()

    assert resource.next_state == "Suspected"
    assert sarko.guilty_found == 0
