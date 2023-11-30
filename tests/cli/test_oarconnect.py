# coding: utf-8
import os

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.cli.oarconnect import cli
from oar.lib.database import ephemeral_session
from oar.lib.models import Queue, Resource

from ..helpers import insert_running_jobs


class Fake_getpwnam(object):
    def __init__(self, user):
        self.pw_shell = "shell"


@pytest.fixture(scope="module", autouse=True)
def set_env(request, backup_and_restore_environ_module):
    os.environ["OARDIR"] = "/tmp"
    os.environ["OARDO_USER"] = "yop"


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
    monkeypatch.setattr(oar.lib.tools, "getpwnam", Fake_getpwnam)


def test_oarconnect_connect(monkeypatch, minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    os.environ["OARDO_USER"] = "oar"
    os.environ["DISPLAY"] = ""
    runner = CliRunner()
    job_id = insert_running_jobs(minimal_db_initialization, 1)[0]

    res = runner.invoke(
        cli,
        [f"{job_id}"],
        obj=(minimal_db_initialization, config),
        catch_exceptions=False,
    )

    assert res.exit_code == 0


def test_oarconnect_connect_bad_user(
    monkeypatch, minimal_db_initialization, setup_config
):
    config, _, _ = setup_config

    os.environ["OARDO_USER"] = "tata"
    os.environ["DISPLAY"] = ""

    runner = CliRunner()
    job_id = insert_running_jobs(minimal_db_initialization, 1, user="toto")[0]

    res = runner.invoke(
        cli,
        [f"{job_id}"],
        obj=(minimal_db_initialization, config),
        catch_exceptions=False,
    )

    out = res.stdout_bytes.decode()
    assert f"#ERROR: User mismatch for job {job_id} (job user is toto)\n" == out
    assert res.exit_code == 20
