# coding: utf-8

import pytest
import zmq
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools
from oar.kao.meta_sched import meta_schedule
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import check_end_of_job, insert_job, set_job_state
from oar.lib.models import AssignedResource, Challenge, Job, Queue, Resource
from oar.modules.bipbip import BipBip

from ..faketools import FakePopen
from ..fakezmq import FakeZmq

NB_RESOURCES = 2
SECURITY_TIME = 60
WALLTIME = 30

fakezmq = FakeZmq()


@pytest.fixture(scope="function", autouse=True)
def builtin_config(request, setup_config):
    config, _ = setup_config
    config.setdefault_config(
        {"CPUSET_PATH": "/oar", "JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD": "cpuset"}
    )
    config["SERVER_HOSTNAME"] = "localhost"
    config["DETACH_JOB_FROM_SERVER"] = "localhost"
    yield config


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, engine = setup_config
    scoped = scoped_session(sessionmaker(bind=engine))
    with ephemeral_session(scoped, engine, bind=engine) as session:
        Queue.create(
            session,
            name="default",
            priority=3,
            scheduler_policy="kamelot",
            state="Active",
        )
        for _ in range(NB_RESOURCES):
            Resource.create(session, network_address="localhost")
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda x, y: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "notify_tcp_socket", lambda addr, port, msg: len(msg)
    )
    monkeypatch.setattr(
        oar.lib.tools, "notify_user", lambda session, job, state, msg: len(state + msg)
    )
    monkeypatch.setattr(oar.lib.tools, "notify_bipbip_commander", lambda j: True)
    monkeypatch.setattr(oar.lib.tools, "pingchecker", lambda hosts: (1, []))
    monkeypatch.setattr(oar.lib.tools, "launch_oarexec", lambda c, d, f: True)
    monkeypatch.setattr(
        oar.lib.tools,
        "manage_remote_commands",
        lambda hosts, data, mf, action, ssh, taktuk_cmd=None: (1, []),
    )
    monkeypatch.setattr(oar.lib.tools, "Popen", FakePopen)
    monkeypatch.setattr(oar.lib.tools, "kill_child_processes", lambda x: None)
    monkeypatch.setattr(zmq, "Context", FakeZmq)
    fakezmq.reset()
    oar.lib.tools.zmq_context = None
    oar.lib.tools.almighty_socket = None
    oar.lib.tools.bipbip_commander_socket = None


class Clock:
    def __init__(self, monkeypatch, t):
        self.now = t
        monkeypatch.setattr(oar.lib.tools, "get_date", lambda session: self.now)


def submit(session, walltime):
    job_id = insert_job(
        session,
        res=[(walltime, [(f"resource_id={NB_RESOURCES}", "")])],
        properties="",
        queue_name="default",
        command="yop",
        stdout_file="poy",
        stderr_file="yop",
    )
    Challenge.create(
        session,
        job_id=job_id,
        challenge="foo1",
        ssh_private_key="foo2",
        ssh_public_key="foo2",
    )
    return job_id


def terminate(session, config, job_id):
    check_end_of_job(
        session, config, job_id, 0, 0, ["localhost"], "toto", "/home/toto", None
    )
    set_job_state(session, config, job_id, "Terminated")


def resources_of(session, job_id):
    return {
        r
        for (r,) in session.query(AssignedResource.resource_id)
        .join(Job, Job.assigned_moldable_job == AssignedResource.moldable_id)
        .filter(Job.id == job_id)
        .all()
    }


def scenario(session, config, monkeypatch, walltime, latency):
    t0 = oar.lib.tools.get_date(session)
    clock = Clock(monkeypatch, t0)

    job1 = submit(session, walltime)
    meta_schedule(session, config)  # job1.start_time = t0

    clock.now = t0 + latency
    BipBip([job1], config=config).run(session, config)  # job1.state -> Running

    j1 = session.query(Job).filter(Job.id == job1).one()
    assert j1.start_time == t0, (
        f"start_time must NOT have been refreshed at launch: "
        f"expected {t0}, got {j1.start_time}"
    )

    occupancy_end = t0 + walltime + latency
    job2_scheduled_at = t0 + walltime + SECURITY_TIME + 1

    if occupancy_end < job2_scheduled_at:
        clock.now = occupancy_end
        terminate(session, config, job1)

    job2 = submit(session, walltime)
    clock.now = job2_scheduled_at
    meta_schedule(session, config)  # place job2

    j2 = session.query(Job).filter(Job.id == job2).one()
    shared = resources_of(session, job1) & resources_of(session, job2)
    state_at_pass = session.query(Job).filter(Job.id == job1).one().state

    still_holding = occupancy_end >= job2_scheduled_at

    # Still occupying: terminate now, AFTER the pass that placed job2.
    if state_at_pass != "Terminated":
        clock.now = occupancy_end
        terminate(session, config, job1)

    j1 = session.query(Job).filter(Job.id == job1).one()

    supposed_end = j1.start_time + walltime + SECURITY_TIME
    real_end = j1.stop_time + SECURITY_TIME
    report = (
        f"latency={latency}s walltime={walltime}s SECURITY_TIME={SECURITY_TIME}s\n"
        f"job {job1}: start_time={j1.start_time} starte={j1.state} "
        f"stop_time={j1.stop_time} \n"
        f"  oarexec launched at {t0 + latency} by BipBip.run()\n"
        f"  job1 holds its resources : [{t0}, {occupancy_end}]\n"
        f"  job2 scheduled at        : {job2_scheduled_at}\n"
        f"  supposed intervals       : [{j1.start_time}, {supposed_end}]\n"
        f"  real window              : [{j1.start_time}, {real_end}]\n"
        f"job {job2}: state={j2.state}, start_time={j2.start_time}\n"
        f"shared resources: {sorted(shared)}"
    )
    return j1, j2, shared, state_at_pass, still_holding, report


@pytest.mark.parametrize("latency", [5, 30, 60, 61, 90, 120])
def test_job1_resources_not_reassigned_while_running(
    monkeypatch, minimal_db_initialization, setup_config, latency
):
    config, _ = setup_config
    j1, j2, shared, state_at_pass, still_holding, report = scenario(
        minimal_db_initialization,
        config,
        monkeypatch,
        walltime=WALLTIME,
        latency=latency,
    )
    if not still_holding:
        # Witness: occupancy was over, job1 had released its resources.
        assert state_at_pass == "Terminated", (
            f"job1 occupancy ended before job2 was placed, expected Terminated, "
            f"got {state_at_pass}\n"
        )
        return
    assert (
        state_at_pass == "Running"
    ), f"job1 should still be holding its resources, got {state_at_pass}\n"
    assert shared == set(), report
