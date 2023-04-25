# coding: utf-8
import re

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.cli.oarstat import cli
from oar.lib.database import ephemeral_session
from oar.lib.models import Accounting, Queue, Resource

from ..helpers import insert_terminated_jobs


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources
        for i in range(10):
            Resource.create(session, network_address="localhost")

        Queue.create(session, name="default")
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "get_date", lambda: 864000)


def test_version(minimal_db_initialization):
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"], obj=minimal_db_initialization)
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_simple_oaraccounting(minimal_db_initialization, setup_config):
    insert_terminated_jobs(minimal_db_initialization)
    runner = CliRunner()
    runner.invoke(cli, obj=minimal_db_initialization)
    accounting = minimal_db_initialization.query(Accounting).all()
    for a in accounting:
        print(
            a.user,
            a.project,
            a.consumption_type,
            a.queue_name,
            a.window_start,
            a.window_stop,
            a.consumption,
        )
    assert accounting[7].consumption == 864000


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oaraccounting_reinitialize(minimal_db_initialization, setup_config):
    insert_terminated_jobs(minimal_db_initialization)
    runner = CliRunner()
    runner.invoke(cli, ["--reinitialize"], obj=minimal_db_initialization)
    accounting = minimal_db_initialization.query(Accounting).all()
    print(accounting)
    assert accounting == []


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oaraccounting_delete_before(
    monkeypatch, minimal_db_initialization, setup_config
):
    insert_terminated_jobs(minimal_db_initialization)
    accounting1 = minimal_db_initialization.query(Accounting).all()
    runner = CliRunner()
    runner.invoke(cli, ["--delete-before", "432000"])
    accounting2 = minimal_db_initialization.query(Accounting).all()

    assert len(accounting1) > len(accounting2)
