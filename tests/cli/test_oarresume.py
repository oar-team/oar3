# coding: utf-8
import os
import re
import pytest

from click.testing import CliRunner

from oar.lib import db, Job, EventLog
from oar.cli.oarresume import cli
from oar.lib.job_handling import insert_job, set_job_state

import oar.lib.tools  # for monkeypatching


@pytest.yield_fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            db["Resource"].create(network_address="localhost")

        db["Queue"].create(name="default")
        yield


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "notify_user", lambda job, state, msg: len(state + msg)
    )


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"])
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oarresume_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 1


def test_oarresume_simple_bad_user():
    os.environ["OARDO_USER"] = "Zorglub"
    job_id = insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)])
    assert result.exit_code == 1


def test_oarresume_simple_bad_nohold():
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)])
    assert result.exit_code == 1


def test_oarresume_simple():
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    set_job_state(job_id, "Suspended")
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)])
    event_job_id = db.query(EventLog.job_id).filter(EventLog.job_id == job_id).one()
    assert event_job_id[0] == job_id
    assert result.exit_code == 0


def test_oarresume_array():
    job_id = insert_job(res=[(60, [("resource_id=4", "")])], array_id=11)
    set_job_state(job_id, "Suspended")
    runner = CliRunner()
    result = runner.invoke(cli, ["--array", "11"])
    event_job_id = db.query(EventLog.job_id).filter(EventLog.job_id == job_id).one()
    assert event_job_id[0] == job_id
    assert result.exit_code == 0


def test_oarresume_array_nojob():
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(res=[(60, [("resource_id=4", "")])])
    runner = CliRunner()
    result = runner.invoke(cli, ["--array", "11"])
    print(result.output)
    assert re.match(r".*job for this array job.*", result.output)
    assert result.exit_code == 0


def test_oarresume_sql():
    job_id = insert_job(res=[(60, [("resource_id=4", "")])], array_id=11)
    set_job_state(job_id, "Suspended")
    runner = CliRunner()
    result = runner.invoke(cli, ["--sql", "array_id='11'"])
    event_job_id = db.query(EventLog.job_id).filter(EventLog.job_id == job_id).one()
    assert event_job_id[0] == job_id
    assert result.exit_code == 0


def test_oarresume_sql_nojob():
    job_id = insert_job(res=[(60, [("resource_id=4", "")])])
    runner = CliRunner()
    result = runner.invoke(cli, ["--sql", "array_id='11'"])
    print(result.output)
    assert re.match(r".*job for this SQL WHERE.*", result.output)
    assert result.exit_code == 0
