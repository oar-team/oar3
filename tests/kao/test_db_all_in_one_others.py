# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.meta_sched import meta_schedule
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import GanttJobsPrediction, Job, Queue, Resource
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

        # add some resources
        for i in range(5):
            Resource.create(session, network_address="localhost" + str(int(i / 2)))
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda x, y: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(oar.lib.tools, "notify_bipbip_commander", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "notify_tcp_socket", lambda addr, port, msg: len(msg)
    )
    monkeypatch.setattr(
        oar.lib.tools, "notify_user", lambda job, state, msg: len(state + msg)
    )


def _test_db_timesharing_1(monkeypatch):
    now = get_date()
    insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", types=["timesharing=*,*"]
    )

    insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", types=["timesharing=*,*"]
    )

    meta_schedule("internal")

    for j in db["Job"].query.all():
        print(j.state)

    res = []
    for i in db["GanttJobsPrediction"].query.all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


def test_db_timesharing_2(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    now = get_date(minimal_db_initialization)
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        types=["timesharing=user,*"],
        user="toto",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        types=["timesharing=user,*"],
        user="titi",
    )
    meta_schedule(minimal_db_initialization, config, "internal")

    for j in minimal_db_initialization.query(Job).all():
        print(j.state)

    res = []
    for i in minimal_db_initialization.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_timesharing_3(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    now = get_date(
        minimal_db_initialization,
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        types=["timesharing=*,*"],
    )

    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        types=["timesharing=*,*"],
    )

    meta_schedule(minimal_db_initialization, config, "internal")

    for j in minimal_db_initialization.query(Job).all():
        print(j.id, j.state)

    res = []
    for i in minimal_db_initialization.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


def test_db_properties_1(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    now = get_date(minimal_db_initialization)
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        properties="network_address='localhost1'",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        properties="network_address='localhost1'",
    )

    meta_schedule(minimal_db_initialization, config, "internal")

    res = []
    for i in minimal_db_initialization.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_properties_2(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    now = get_date(minimal_db_initialization)
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "network_address='localhost1'")])],
        properties="",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "network_address='localhost1'")])],
        properties="",
    )

    meta_schedule(minimal_db_initialization, config, "internal")

    for j in minimal_db_initialization.query(Job).all():
        print(j.state)

    res = []
    for i in minimal_db_initialization.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_properties_3(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    now = get_date(minimal_db_initialization)
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "network_address='localhost0'")])],
        properties="network_address='localhost0'",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "network_address='localhost1'")])],
        properties="network_address='localhost1'",
    )

    meta_schedule(minimal_db_initialization, config, "internal")

    res = []
    for i in minimal_db_initialization.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


def _test_db_placeholder_1(monkeypatch, minimal_db_initialization, setup_config):
    now = get_date()
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        types=["placeholder=yop"],
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        types=["allow=yop"],
    )

    meta_schedule(minimal_db_initialization, config, "internal")

    for j in db["Job"].query.all():
        print(j.state)

    res = []
    for i in minimal_db_initialization.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


def test_db_placeholder_2(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    now = get_date(minimal_db_initialization)
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        types=["placeholder=yop"],
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        types=["allow=poy"],
    )

    meta_schedule(minimal_db_initialization, config, "internal")

    res = []
    for i in minimal_db_initialization.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_moldable_1(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    now = get_date(
        minimal_db_initialization,
    )
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=3", "")])], properties=""
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")]), (70, [("resource_id=3", "")])],
        properties="",
    )

    meta_schedule(minimal_db_initialization, config, "internal")

    res = []
    for i in minimal_db_initialization.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_moldable_2(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    now = get_date(
        minimal_db_initialization,
    )
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=3", "")])], properties=""
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")]), (70, [("resource_id=2", "")])],
        properties="",
    )

    meta_schedule(minimal_db_initialization, config, "internal")

    res = []
    for i in minimal_db_initialization.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


def test_db_suspended_duration_1(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=3", "")])],
        properties="",
        suspended="YES",
    )

    meta_schedule(minimal_db_initialization, config, "internal")

    job = minimal_db_initialization.query(Job).one()
    assert job.state == "toLaunch"
    # set_job_state(job.id, 'Running')
