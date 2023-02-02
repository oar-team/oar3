# coding: utf-8
import re

import pytest
from click.testing import CliRunner

import oar.lib.tools  # for monkeypatching
from oar.cli.oaraccounting import cli
from oar.lib import Accounting, db

from ..helpers import insert_terminated_jobs


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(10):
            db["Resource"].create(network_address="localhost")

        db["Queue"].create(name="default")
        yield


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "get_date", lambda: 864000)


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"])
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_simple_oaraccounting():
    insert_terminated_jobs()
    runner = CliRunner()
    runner.invoke(cli)
    accounting = db.query(Accounting).all()
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
def test_oaraccounting_reinitialize():
    insert_terminated_jobs()
    runner = CliRunner()
    runner.invoke(cli, ["--reinitialize"])
    accounting = db.query(Accounting).all()
    print(accounting)
    assert accounting == []


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oaraccounting_delete_before(monkeypatch):
    insert_terminated_jobs()
    accounting1 = db.query(Accounting).all()
    runner = CliRunner()
    runner.invoke(cli, ["--delete-before", "432000"])
    accounting2 = db.query(Accounting).all()

    assert len(accounting1) > len(accounting2)
