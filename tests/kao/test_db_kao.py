# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.kao import main
from oar.lib.database import ephemeral_session
from oar.lib.globals import get_logger, init_oar
from oar.lib.job_handling import insert_job
from oar.lib.models import Job, Queue, Resource

config, engine, log = init_oar(no_db=True)

logger = get_logger("oar.kao")


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config["METASCHEDULER_MODE"] = "internal"

    @request.addfinalizer
    def teardown():
        config["METASCHEDULER_MODE"]


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
        oar.lib.tools, "notify_user", lambda job, state, msg: len(state + msg)
    )
    monkeypatch.setattr(oar.lib.tools, "notify_bipbip_commander", lambda json_msg: True)


def test_db_kao_simple_1(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    job = minimal_db_initialization.query(Job).one()
    print("job state:", job.state)

    main(minimal_db_initialization, config)

    job = minimal_db_initialization.query(Job).one()

    assert job.state == "toLaunch"


def test_db_kao_moldable(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    # First moldable job should never pass because there is not enough resources.
    insert_job(
        minimal_db_initialization,
        res=[(5, [("resource_id=6", "")]), (2, [("resource_id=2", "")])],
        properties="",
    )
    job = minimal_db_initialization.query(Job).one()

    print("job state:", job.state)

    main(minimal_db_initialization, config)

    job = minimal_db_initialization.query(Job).one()
    print(job.state)

    assert job.state == "toLaunch"
