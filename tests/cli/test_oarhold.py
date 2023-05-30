# coding: utf-8
import os
import re

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.cli.oarhold import cli
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


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)


def test_version(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"], obj=(minimal_db_initialization, config))
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oarhold_void(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, obj=(minimal_db_initialization, config))
    assert result.exit_code == 1


def test_oarhold_simple_bad_user(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "Zorglub"
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)], obj=(minimal_db_initialization, config))
    assert result.exit_code == 1


def test_oarhold_simple(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)], obj=(minimal_db_initialization, config))
    event_job_id = (
        minimal_db_initialization.query(EventLog.job_id)
        .filter(EventLog.job_id == job_id)
        .one()
    )
    assert event_job_id[0] == job_id
    assert result.exit_code == 0


def test_oarhold_array(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], array_id=11
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--array", "11"], obj=(minimal_db_initialization, config)
    )
    event_job_id = (
        minimal_db_initialization.query(EventLog.job_id)
        .filter(EventLog.job_id == job_id)
        .one()
    )
    assert event_job_id[0] == job_id
    assert result.exit_code == 0


def test_oarhold_array_nojob(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    insert_job(minimal_db_initialization, res=[(60, [("resource_id=4", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--array", "11"], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    assert re.match(r".*job for this array job.*", result.output)
    assert result.exit_code == 0


def test_oarhold_sql(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], array_id=11
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--sql", "array_id='11'"], obj=(minimal_db_initialization, config)
    )
    event_job_id = (
        minimal_db_initialization.query(EventLog.job_id)
        .filter(EventLog.job_id == job_id)
        .one()
    )
    assert event_job_id[0] == job_id
    assert result.exit_code == 0


def test_oarhold_sql_nojob(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    insert_job(minimal_db_initialization, res=[(60, [("resource_id=4", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--sql", "array_id='11'"], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    assert re.match(r".*job for this SQL WHERE.*", result.output)
    assert result.exit_code == 0


def test_oarhold_job_types_cosystem(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        types=["cosystem"],
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--running", str(job_id)], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    assert re.match(r".*cosystem type.*", result.output)
    assert result.exit_code == 2


def test_oarhold_job_types_deploy(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], types=["deploy"]
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--running", str(job_id)], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    assert re.match(r".*deploy type.*", result.output)
    assert result.exit_code == 2
