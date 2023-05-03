import os
import re

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.cli.oarqueue import cli
from oar.lib.database import ephemeral_session
from oar.lib.models import Queue
from oar.lib.queue import create_queue


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:

        Queue.create(session, name="default", scheduler_policy="kao", state="unkown")
        yield session


def test_version(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"], obj=(minimal_db_initialization, config))
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oarqueue_bad_user(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "bob"
    runner = CliRunner()
    result = runner.invoke(cli, obj=(minimal_db_initialization, config))
    assert result.exit_code == 8


def test_oarqueue_void(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, obj=(minimal_db_initialization, config))
    print(result.output)
    assert result.exit_code == 0
    assert re.match(r".*default.*", result.output)


def test_oarqueue_enable(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-e", "default"], obj=(minimal_db_initialization, config)
    )
    assert result.exit_code == 0
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "Active"


def test_oarqueue_disable(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-d", "default"], obj=(minimal_db_initialization, config)
    )
    assert result.exit_code == 0
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "notActive"


def test_oarqueue_enable_all(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, ["-E"], obj=(minimal_db_initialization, config))
    assert result.exit_code == 0
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "Active"


def test_oarqueue_disable_all(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, ["-D"], obj=(minimal_db_initialization, config))
    assert result.exit_code == 0
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "notActive"


def test_oarqueue_add(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--add", "admin,10,kamelot"], obj=(minimal_db_initialization, config)
    )
    assert result.exit_code == 0
    queue = (
        minimal_db_initialization.query(Queue)
        .filter(Queue.scheduler_policy == "kamelot")
        .one()
    )
    assert queue.name == "admin"


def test_oarqueue_change(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--change", "default,42,fast"], obj=(minimal_db_initialization, config)
    )
    assert result.exit_code == 0
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.priority == 42
    assert queue.scheduler_policy == "fast"


def test_oarqueue_remove(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    create_queue(minimal_db_initialization, "admin", 10, "kamelot")
    assert len(minimal_db_initialization.query(Queue).all()) == 2
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--remove", "admin"], obj=(minimal_db_initialization, config)
    )
    assert result.exit_code == 0
    assert len(minimal_db_initialization.query(Queue).all()) == 1
