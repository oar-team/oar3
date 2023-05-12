# coding: utf-8
import sys
import time

import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.kamelot import main
from oar.lib.database import ephemeral_session
from oar.lib.globals import init_oar
from oar.lib.job_handling import insert_job
from oar.lib.models import GanttJobsPrediction, GanttJobsResource, Resource


@pytest.fixture(scope="function", autouse=False)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        for i in range(5):
            Resource.create(session, network_address="localhost")

        for i in range(5):
            insert_job(session, res=[(60, [("resource_id=2", "")])], properties="")
        yield session


def test_db_kamelot_1(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    old_sys_argv = sys.argv
    sys.argv = ["test_kamelot", "default", time.time()]
    main(session=minimal_db_initialization, config=config)
    sys.argv = old_sys_argv
    req = minimal_db_initialization.query(GanttJobsPrediction).all()
    assert len(req) == 5


def test_db_kamelot_2(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    old_sys_argv = sys.argv
    sys.argv = ["test_kamelot", "default"]
    main(session=minimal_db_initialization, config=config)
    sys.argv = old_sys_argv
    req = minimal_db_initialization.query(GanttJobsPrediction).all()
    assert len(req) == 5


def test_db_kamelot_3(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    old_sys_argv = sys.argv
    sys.argv = ["test_kamelot"]
    main(session=minimal_db_initialization, config=config)
    sys.argv = old_sys_argv
    req = minimal_db_initialization.query(GanttJobsPrediction).all()
    assert len(req) == 5


@pytest.fixture(scope="function", autouse=False)
def properties_init(request, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    for i in range(4):
        Resource.create(minimal_db_initialization, network_address="localhost")

    for i in range(3):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=2", "")])],
            properties="",
        )

    tokens = [
        Resource.create(minimal_db_initialization, type="token").id,
        Resource.create(minimal_db_initialization, type="token").id,
    ]

    yield (tokens, minimal_db_initialization)


def test_db_kamelot_4(properties_init, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    properties_init, session = properties_init
    old_sys_argv = sys.argv
    sys.argv = ["test_kamelot", "default", time.time()]
    main(session=minimal_db_initialization, config=config)
    sys.argv = old_sys_argv
    req = session.query(GanttJobsResource).all()

    for alloc in req:
        assert alloc.resource_id not in properties_init
