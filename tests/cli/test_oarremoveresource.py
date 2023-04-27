# coding: utf-8
import os

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.cli.oarremoveresource import cli
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import EventLog, Queue, Resource


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources
        for i in range(5):
            Resource.create(session, network_address="localhost")

        Queue.create(session, name="default")
        yield session


def test_oarremoveresource_void(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, obj=(config, minimal_db_initialization))
    assert result.exit_code == 2


def test_oarremoveresource_bad_user(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "Zorglub"
    runner = CliRunner()
    result = runner.invoke(cli, ["1"], obj=(config, minimal_db_initialization))
    assert result.exit_code == 4


def test_oarremoveresource_not_dead(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    first_id = db.query(Resource).first().id
    runner = CliRunner()
    result = runner.invoke(
        cli, [str(first_id)], obj=(config, minimal_db_initialization)
    )
    assert result.exit_code == 3


def test_oarremoveresource_no_resource(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, obj=(config, minimal_db_initialization))
    assert result.exit_code == 2


def test_oarremoveresource_simple(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    Resource.create(
        minimal_db_initialization, network_address="localhost", state="Dead"
    )
    nb_res1 = len(minimal_db_initialization.query(Resource).all())
    first_id = minimal_db_initialization.query(Resource).first().id
    dead_rid = first_id + 5

    result = runner.invoke(
        cli, [str(dead_rid)], obj=(config, minimal_db_initialization)
    )
    nb_res2 = len(db.query(Resource).all())
    assert nb_res1 == 6
    assert nb_res2 == 5
    assert result.exit_code == 0
