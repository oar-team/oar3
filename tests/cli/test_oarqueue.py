import os
import re

import pytest
from click.testing import CliRunner

from oar.cli.oarqueue import cli
from oar.lib import Queue, db
from oar.lib.queue import create_queue


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        Queue.create(name="default", scheduler_policy="kao", state="unkown")
        yield


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"])
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oarqueue_bad_user():
    os.environ["OARDO_USER"] = "bob"
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 8


def test_oarqueue_void():
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli)
    print(result.output)
    assert result.exit_code == 0
    assert re.match(r".*default.*", result.output)


def test_oarqueue_enable():
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, ["-e", "default"])
    assert result.exit_code == 0
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "Active"


def test_oarqueue_disable():
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, ["-d", "default"])
    assert result.exit_code == 0
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "notActive"


def test_oarqueue_enable_all():
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, ["-E"])
    assert result.exit_code == 0
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "Active"


def test_oarqueue_disable_all():
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, ["-D"])
    assert result.exit_code == 0
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "notActive"


def test_oarqueue_add():
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, ["--add", "admin,10,kamelot"])
    assert result.exit_code == 0
    queue = db.query(Queue).filter(Queue.scheduler_policy == "kamelot").one()
    assert queue.name == "admin"


def test_oarqueue_change():
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli, ["--change", "default,42,fast"])
    assert result.exit_code == 0
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.priority == 42
    assert queue.scheduler_policy == "fast"


def test_oarqueue_remove():
    os.environ["OARDO_USER"] = "oar"
    create_queue("admin", 10, "kamelot")
    assert len(db.query(Queue).all()) == 2
    runner = CliRunner()
    result = runner.invoke(cli, ["--remove", "admin"])
    assert result.exit_code == 0
    assert len(db.query(Queue).all()) == 1
