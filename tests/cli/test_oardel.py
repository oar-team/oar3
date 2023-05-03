# coding: utf-8
import os
import re

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.cli.oardel import cli
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import FragJob, JobStateLog, Queue, Resource


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
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(oar.lib.tools, "signal_oarexec", lambda *x: 0)


def test_version(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"], obj=(minimal_db_initialization, config))
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oardel_void(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, obj=(minimal_db_initialization, config))
    assert result.exit_code == 1


def test_oardel_simple(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)], obj=(minimal_db_initialization, config))
    fragjob_id = (
        minimal_db_initialization.query(FragJob.job_id)
        .filter(FragJob.job_id == job_id)
        .one()
    )
    assert fragjob_id[0] == job_id
    assert result.exit_code == 0


def test_oardel_simple_cosystem(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        types=["cosystem"],
        state="Running",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-s", "USR1", str(job_id)], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    import traceback

    print(traceback.format_tb(result.exc_info[2]))
    assert result.exit_code == 0


def test_oardel_simple_deploy(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        types=["deploy"],
        state="Running",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-s", "USR1", str(job_id)], obj=(minimal_db_initialization, config)
    )
    assert result.exit_code == 0


def test_oardel_simple_bad_user(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "Zorglub"
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)], obj=(minimal_db_initialization, config))
    assert result.exit_code == 1


def test_oardel_array(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    array_id = 1234  # Arbitrarily chosen
    for _ in range(5):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
            array_id=array_id,
            user="toto",
        )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--array", "1234"], obj=(minimal_db_initialization, config)
    )
    assert result.exit_code == 0
    assert len(minimal_db_initialization.query(FragJob.job_id).all()) == 5


def test_oardel_array_nojob(minimal_db_initialization, setup_config):
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


def test_oardel_sql(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], array_id=11
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--sql", "array_id='11'"], obj=(minimal_db_initialization, config)
    )
    assert result.exit_code == 0
    assert len(minimal_db_initialization.query(FragJob.job_id).all()) == 1


def test_oardel_sql_nojob(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    insert_job(minimal_db_initialization, res=[(60, [("resource_id=4", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--sql", "array_id='11'"], obj=(minimal_db_initialization, config)
    )
    assert re.match(r".*job for this SQL WHERE.*", result.output)
    assert result.exit_code == 0


def test_oardel_force_terminate_finishing_job_bad_user(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "Zorglub"
    job_id = insert_job(minimal_db_initialization, res=[(60, [("resource_id=4", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--force-terminate-finishing-job", str(job_id)],
        obj=(minimal_db_initialization, config),
    )
    assert result.exit_code == 8


def test_oardel_force_terminate_finishing_job_not_finishing(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(minimal_db_initialization, res=[(60, [("resource_id=4", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--force-terminate-finishing-job", str(job_id)],
        obj=(minimal_db_initialization, config),
    )
    assert result.exit_code == 10


def test_oardel_force_terminate_finishing_job_too_early(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        state="Finishing",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--force-terminate-finishing-job", str(job_id)],
        obj=(minimal_db_initialization, config),
    )
    assert result.exit_code == 11


def test_oardel_force_terminate_finishing_job(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        state="Finishing",
    )
    minimal_db_initialization.execute(
        JobStateLog.__table__.insert(),
        {"job_id": job_id, "job_state": "Finishing", "date_start": 0, "date_stop": 50},
    )
    minimal_db_initialization.execute(
        JobStateLog.__table__.insert(),
        {"job_id": job_id, "job_state": "Finishing", "date_start": 100},
    )
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--force-terminate-finishing-job", str(job_id)],
        obj=(minimal_db_initialization, config),
    )

    print(result.output)
    assert re.match(r".*Force the termination.*", result.output)
    assert result.exit_code == 0


def test_oardel_besteffort_bad_user(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "Zorglub"
    job_id = insert_job(minimal_db_initialization, res=[(60, [("resource_id=4", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--besteffort", str(job_id)], obj=(minimal_db_initialization, config)
    )
    assert result.exit_code == 8


def test_oardel_besteffort_not_running(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(minimal_db_initialization, res=[(60, [("resource_id=4", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--besteffort", str(job_id)], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    assert re.match(r".*Running state.*", result.output)
    assert result.exit_code == 0


def test_oardel_remove_besteffort(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        state="Running",
        types=["besteffort"],
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--besteffort", str(job_id)], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    assert re.match(r".*Remove besteffort type.*", result.output)
    assert result.exit_code == 0


def test_oardel_add_besteffort(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], state="Running"
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--besteffort", str(job_id)], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    assert re.match(r".*Add besteffort type .*", result.output)
    assert result.exit_code == 0
