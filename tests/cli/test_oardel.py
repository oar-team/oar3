# coding: utf-8
import os
import pytest

from click.testing import CliRunner

from oar.lib import (db, FragJob, Job)
from oar.cli.oardel import cli
from oar.kao.job import insert_job
                         
import oar.lib.tools  # for monkeypatching

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: len(x))


def test_oardel_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 1

    
def test_oardel_simple():
    os.environ['OARDO_USER'] = 'oar'
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)])
    fragjob_id = db.query(FragJob.job_id).filter(FragJob.job_id == job_id).one()
    assert fragjob_id[0] == job_id
    assert result.exit_code == 0

def test_oardel_simple_bad_user():
    os.environ['OARDO_USER'] = 'Zorglub'
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)])
    assert result.exit_code == 1

def test_oardel_array():
    os.environ['OARDO_USER'] = 'oar'
    array_id = 1234 # Arbitrarily chosen
    for _ in range(5):
        insert_job(res=[(60, [('resource_id=4', "")])], properties="", array_id=array_id, user='toto')
    runner = CliRunner()
    result = runner.invoke(cli, ['--array', '1234'])
    assert result.exit_code == 0
    assert len(db.query(FragJob.job_id).all()) == 5
    
