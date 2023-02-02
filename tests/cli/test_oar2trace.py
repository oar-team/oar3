# coding: utf-8
import pytest
from click.testing import CliRunner

from oar.cli.oar2trace import cli
from oar.kao.meta_sched import meta_schedule
from oar.lib import db
from oar.lib.job_handling import insert_job, set_job_state

NB_NODES = 5


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(NB_NODES):
            db["Resource"].create(network_address="localhost")
        yield


def test_oar2trace_void():
    runner = CliRunner()
    result = runner.invoke(cli, ["-p"])
    assert result.exit_code == 1


@pytest.mark.skip(reason="wip (not working)")
def test_oar2trace_simple():
    insert_job(res=[(100, [("resource_id=3", "")])])

    meta_schedule("internal")
    job = db["Job"].query.one()

    set_job_state(job.id, "Terminated")

    runner = CliRunner()
    result = runner.invoke(cli, ["-p"])
    assert result.exit_code == 0

    import pdb

    pdb.set_trace()
