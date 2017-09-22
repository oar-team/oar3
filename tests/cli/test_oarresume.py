# coding: utf-8
import os
import pytest

from click.testing import CliRunner

from oar.lib import (db, Job, EventLog)
from oar.cli.oarresume import cli
from oar.lib.job_handling import (insert_job, set_job_state)

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
    monkeypatch.setattr(oar.lib.tools, 'notify_user', lambda job, state, msg: len(state + msg))

def test_oarresume_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 1

def test_oarresume_simple_bad_user():
    os.environ['OARDO_USER'] = 'Zorglub'
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)])
    assert result.exit_code == 1

def test_oarresume_simple_bad_nohold():
    os.environ['OARDO_USER'] = 'oar'
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)])
    assert result.exit_code == 1


def test_oarresume_simple():
    os.environ['OARDO_USER'] = 'oar'
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    set_job_state(job_id, 'Suspended')
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)])
    event_job_id = db.query(EventLog.job_id).filter(EventLog.job_id == job_id).one()
    assert event_job_id[0] == job_id
    assert result.exit_code == 0



