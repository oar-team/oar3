# coding: utf-8
import os
import time
from codecs import open
from datetime import datetime, timedelta
from tempfile import mkstemp

import pytest
import zmq
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.meta_sched import meta_schedule
from oar.kao.quotas import Quotas
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import Job, Queue, Resource
from oar.lib.node import get_next_job_date_on_node
from oar.lib.tools import get_date

from ..fakezmq import FakeZmq

# import pdb

node_list = []

fakezmq = FakeZmq()
fakezmq.reset()

quotas_simple_temporal_rules = {
    "periodical": [["*,*,*,*", "quotas_1", "default"]],
    "quotas_1": {"*,*,*,/": [1, -1, -1]},
    "quotas_2": {"*,*,*,/": [-1, -1, -1]},
}


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        Queue.create(
            session,
            name="default",
            priority=0,
            scheduler_policy="kamelot",
            state="Active",
        )
        Queue.create(
            session,
            name="admin",
            priority=100,
            scheduler_policy="kamelot",
            state="Active",
        )
        # add some resources
        for i in range(10):
            Resource.create(session, network_address="localhost" + str(int(i / 3)))

        yield session


@pytest.fixture(scope="function")
def active_quotas(request, setup_config):
    config, _, _ = setup_config
    config["QUOTAS"] = "yes"
    _, quotas_file_name = mkstemp()
    config["QUOTAS_CONF_FILE"] = quotas_file_name

    def teardown():
        Quotas.enabled = False
        Quotas.calendar = None
        Quotas.default_rules = {}
        Quotas.job_types = ["*"]
        config["QUOTAS"] = "no"
        os.remove(config["QUOTAS_CONF_FILE"])
        del config["QUOTAS_CONF_FILE"]

    request.addfinalizer(teardown)

    yield config


@pytest.fixture(scope="function")
def active_energy_saving(request, setup_config):
    config, _, _ = setup_config
    # Some tests modify this value. We register the initial value and reset it
    # after the test so it doesn't break other tests.
    initial_energy_saving_internal = config["ENERGY_SAVING_INTERNAL"]
    config["ENERGY_SAVING_MODE"] = "metascheduler_decision_making"
    config["SCHEDULER_NODE_MANAGER_SLEEP_CMD"] = "sleep_node_command"
    config["SCHEDULER_NODE_MANAGER_SLEEP_TIME"] = "15"
    config["SCHEDULER_NODE_MANAGER_IDLE_TIME"] = "30"
    config["SCHEDULER_NODE_MANAGER_WAKEUP_TIME"] = "30"
    config["SCHEDULER_NODE_MANAGER_WAKE_UP_CMD"] = "wakeup_node_command"

    yield config

    def teardown():
        # config.clear()
        # config.update(config.DEFAULT_CONFIG)

        del config["SCHEDULER_NODE_MANAGER_SLEEP_CMD"]
        del config["SCHEDULER_NODE_MANAGER_SLEEP_TIME"]
        del config["SCHEDULER_NODE_MANAGER_IDLE_TIME"]
        del config["SCHEDULER_NODE_MANAGER_WAKEUP_TIME"]
        del config["SCHEDULER_NODE_MANAGER_WAKE_UP_CMD"]
        del config["ENERGY_SAVING_MODE"]
        config["ENERGY_SAVING_INTERNAL"] = initial_energy_saving_internal

    request.addfinalizer(teardown)


def period_weekstart():
    t_dt = datetime.fromtimestamp(time.time()).date()
    t_weekstart_day_dt = t_dt - timedelta(days=t_dt.weekday())
    return int(datetime.combine(t_weekstart_day_dt, datetime.min.time()).timestamp())


def create_quotas_rules_file(config, quotas_rules):
    """create_quotas_rules_file('{"quotas": {"*,*,*,toto": [1,-1,-1],"*,*,*,john": [150,-1,-1]}}')"""
    with open(config["QUOTAS_CONF_FILE"], "w", encoding="utf-8") as quotas_fd:
        quotas_fd.write(quotas_rules)
    Quotas.enable(config)


def insert_and_sched_ar(session, config, start_time, walltime=60, user=""):
    insert_job(
        session,
        user=user,
        res=[(walltime, [("resource_id=4", "")])],
        reservation="toSchedule",
        start_time=start_time,
        info_type="localhost:4242",
    )

    meta_schedule(session, config, "internal")

    return session.query(Job).order_by(Job.id.desc()).first()


def assign_node_list(nodes):
    global node_list
    print("lol")
    node_list = nodes


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda x, y: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "notify_tcp_socket", lambda addr, port, msg: len(msg)
    )
    monkeypatch.setattr(
        oar.lib.tools, "notify_user", lambda job, state, msg: len(state + msg)
    )
    monkeypatch.setattr(
        oar.lib.tools,
        "fork_and_feed_stdin",
        lambda cmd, timeout_cmd, nodes: assign_node_list(nodes),
    )
    monkeypatch.setattr(oar.lib.tools, "send_checkpoint_signal", lambda job: None)
    monkeypatch.setattr(zmq, "Context", FakeZmq)
    monkeypatch.setattr(oar.lib.tools, "notify_bipbip_commander", lambda json_msg: True)


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    fakezmq.reset()
    oar.lib.tools.zmq_context = None
    oar.lib.tools.almighty_socket = None
    oar.lib.tools.bipbip_commander_socket = None


def test_db_all_in_one_wakeup_node_1(
    monkeypatch, minimal_db_initialization, setup_config, active_energy_saving
):
    config = active_energy_saving
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )

    now = get_date(minimal_db_initialization)
    # Suspend nodes
    minimal_db_initialization.query(Resource).update(
        {Resource.state: "Absent", Resource.available_upto: now + 1000},
        synchronize_session=False,
    )
    minimal_db_initialization.commit()
    meta_schedule(minimal_db_initialization, config, "internal")

    job = minimal_db_initialization.query(Job).one()
    print(job.state)
    print(node_list)
    assert job.state == "Waiting"
    assert node_list == ["localhost0", "localhost1"]


def test_wakeup_node_2(
    monkeypatch, minimal_db_initialization, setup_config, active_energy_saving
):
    config = active_energy_saving
    insert_job(
        minimal_db_initialization,
        res=[
            (
                60,
                [
                    (
                        "network_address=2/resource_id=1",
                        "network_address != 'localhost1'",
                    )
                ],
            )
        ],
        properties="",
    )

    now = get_date(minimal_db_initialization)
    # Suspend nodes
    minimal_db_initialization.query(Resource).update(
        {Resource.state: "Absent", Resource.available_upto: now + 1000},
        synchronize_session=False,
    )
    minimal_db_initialization.commit()
    meta_schedule(minimal_db_initialization, config, "internal")

    job = minimal_db_initialization.query(Job).one()
    print(job.state)
    print(node_list)
    assert job.state == "Waiting"
    assert node_list == ["localhost0", "localhost2"]


def test_sleep_two_network_address(
    monkeypatch, minimal_db_initialization, setup_config, active_energy_saving
):
    config = active_energy_saving

    now = get_date(minimal_db_initialization)

    # This resources description should push the job on two different network_address
    # Leading localhost0 and localhost1 to be used
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=3", "resource_id != 1")])],
        properties="",
    )

    minimal_db_initialization.query(Resource).update(
        {Resource.available_upto: now + 50000}, synchronize_session=False
    )
    minimal_db_initialization.commit()
    meta_schedule(minimal_db_initialization, config, "internal")

    # print([(r.id, r.network_address) for r in minimal_db_initialization.query(Resource).all()])

    job = minimal_db_initialization.query(Job).one()
    print(f"state: {job.state} - node_list: {node_list}")
    print(
        [
            (r.network_address, r.id)
            for r in minimal_db_initialization.query(Resource).all()
        ]
    )

    assert job.state == "toLaunch"
    assert set(node_list) == set(["localhost2", "localhost3"])


def test_next_job_date_on_node(
    monkeypatch, minimal_db_initialization, setup_config, active_energy_saving
):
    config = active_energy_saving

    now = get_date(minimal_db_initialization)

    # This resources description should push the job on two different network_address
    # Leading localhost0 and localhost1 to be used
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=3", "resource_id != 1")])],
        properties="",
    )

    minimal_db_initialization.query(Resource).update(
        {Resource.available_upto: now + 50000}, synchronize_session=False
    )
    minimal_db_initialization.commit()
    meta_schedule(minimal_db_initialization, config, "internal")

    job = minimal_db_initialization.query(Job).one()

    res = get_next_job_date_on_node(minimal_db_initialization, "localhost1")
    assert job.start_time == res

    res = get_next_job_date_on_node(minimal_db_initialization, "localhost0")
    assert job.start_time == res

    res = get_next_job_date_on_node(minimal_db_initialization, "localhost2")
    assert res is None


def test_db_metasched_ar_1(monkeypatch, minimal_db_initialization, setup_config):
    # add one job
    config, _, _ = setup_config
    now = get_date(minimal_db_initialization)
    print(f"now: {now}")
    # sql_now = local_to_sql(now)

    insert_job(
        minimal_db_initialization,
        res=[(60, [("network_address=2/resource_id=2", "resource_id != 1")])],
        properties="",
        reservation="toSchedule",
        start_time=(now + 10),
        info_type="localhost:4242",
    )

    meta_schedule(minimal_db_initialization, config)

    # Calling twice so the AR jobs scheduled the first time will show up in
    # the initialization functions
    meta_schedule(minimal_db_initialization, config)

    print(
        [
            (r.network_address, r.id)
            for r in minimal_db_initialization.query(Resource).all()
        ]
    )
    job = minimal_db_initialization.query(Job).all()
    print(job)
