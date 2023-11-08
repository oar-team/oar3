# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.kao.kamelot_basic import main
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import GanttJobsPrediction, Resource


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        yield session


def test_db_kamelot_basic_1(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    # add some resources
    for i in range(5):
        Resource.create(minimal_db_initialization, network_address="localhost")

    for i in range(5):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=2", "")])],
            properties="",
        )

    main(minimal_db_initialization, config)

    req = minimal_db_initialization.query(GanttJobsPrediction).all()

    for i, r in enumerate(req):
        print("req:", r.moldable_id, r.start_time)

    assert len(req) == 5
