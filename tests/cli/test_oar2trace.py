# coding: utf-8
import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.cli.oar2trace import cli
from oar.kao.meta_sched import meta_schedule
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job, set_job_state
from oar.lib.models import Job, Resource

NB_NODES = 5


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources
        for i in range(NB_NODES):
            Resource.create(session, network_address="localhost")

        yield session


def test_oar2trace_void(minimal_db_initialization, setup_config):
    config, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-p"])
    assert result.exit_code == 1


# flake8: noqa: F401 (TODO)
@pytest.mark.skip(reason="wip (not working)")
def test_oar2trace_simple(minimal_db_initialization, setup_config):
    config, _ = setup_config
    insert_job(minimal_db_initialization, res=[(100, [("resource_id=3", "")])])

    meta_schedule(minimal_db_initialization, config)

    job = minimal_db_initialization.query(Job).one()

    set_job_state(minimal_db_initialization, config, job.id, "Terminated")

    runner = CliRunner()
    result = runner.invoke(cli, ["-p"])
    assert result.exit_code == 0
