# coding: utf-8
import sys

import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.kamelot import get_max_job_per_queues_group, main
from oar.kao.meta_sched import meta_schedule
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import GanttJobsPrediction, Queue, Resource

OPT = "MAX_JOB_PER_QUEUES_GROUP_SCHEDULING_ROUND"


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, engine = setup_config
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
        for i in range(10):
            Resource.create(session, network_address="localhost")
        for i in range(5):
            insert_job(session, res=[(60, [("resource_id=2", "")])], properties="")
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda x, y: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "notify_tcp_socket", lambda addr, port, msg: len(msg)
    )


# --- parsing (pure function, no DB) -----------------------------------------
def test_get_max_job_per_queues_group_parsing():
    assert get_max_job_per_queues_group({}) is None
    assert get_max_job_per_queues_group({OPT: ""}) is None
    assert get_max_job_per_queues_group({OPT: "0"}) is None
    assert get_max_job_per_queues_group({OPT: "-1"}) is None
    assert get_max_job_per_queues_group({OPT: "abc"}) is None
    assert get_max_job_per_queues_group({OPT: "1000"}) == 1000


# --- standalone path: main() -> schedule_cycle ------------------------------
def test_db_kamelot_max_job_no_limit(minimal_db_initialization, setup_config):
    config, _ = setup_config
    old = sys.argv
    sys.argv = ["test_kamelot", "default"]
    main(session=minimal_db_initialization, config=config)
    sys.argv = old
    req = minimal_db_initialization.query(GanttJobsPrediction).all()
    assert len(req) == 5


def test_db_kamelot_max_job_schedule_cycle(minimal_db_initialization, setup_config):
    config, _ = setup_config
    config[OPT] = "2"
    old = sys.argv
    sys.argv = ["test_kamelot", "default"]
    main(session=minimal_db_initialization, config=config)
    sys.argv = old
    del config[OPT]
    req = minimal_db_initialization.query(GanttJobsPrediction).all()
    assert len(req) == 2


# --- production path: meta_schedule() -> internal_schedule_cycle -------------
def test_db_metasched_max_job_internal_cycle(minimal_db_initialization, setup_config):
    config, _ = setup_config
    config[OPT] = "2"
    meta_schedule(minimal_db_initialization, config)
    del config[OPT]
    req = minimal_db_initialization.query(GanttJobsPrediction).all()
    assert len(req) == 2
