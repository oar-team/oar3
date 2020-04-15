# coding: utf-8
import re
import os
import pytest

from ..helpers import insert_running_jobs

from click.testing import CliRunner

from oar.lib import (db, config, WalltimeChange)
from oar.cli.oarwalltime import cli    
from oar.lib.job_handling import insert_job

import oar.lib.tools  # for monkeypatching

fake_notifications = []

def fake_notify_almighty(notification):
    global fake_notifications
    fake_notifications.append(notification)
    
@pytest.yield_fixture(scope='function')
def minimal_db_initialization(request):
    global fake_notifications
    fake_notifications = []
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost")
        db['Queue'].create(name='default')
        yield
        
@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', fake_notify_almighty)

@pytest.fixture(scope='function', autouse=True)
def finalizer(request):
    @request.addfinalizer
    def teardown():
        if 'OARDO_USER' in os.environ:
            del os.environ['OARDO_USER']

def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ['-V'])
    print(result.output)
    assert re.match(r'.*\d\.\d\.\d.*', result.output)

def test_oardel_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 1

def test_oardel_disabled():
    runner = CliRunner()
    result = runner.invoke(cli, ['666666'])
    print(result.output)
    assert result.exit_code == 1
    assert re.match(r'.*functionality is disabled.*', result.output)
    
def test_oardel_unexisting_job():
    config['WALLTIME_CHANGE_ENABLED'] = 'YES'
    runner = CliRunner()
    result = runner.invoke(cli, ['666666'])
    print(result.output)
    assert result.exit_code == 1
    assert re.match(r'.*unknown job.*', result.output)
                    
def test_oardel_not_running_job1():
    config['WALLTIME_CHANGE_ENABLED'] = 'YES'
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id)])
    print(result.output)
    assert result.exit_code == 1
    assert re.match(r'.*job is not running yet.*', result.output)

def test_oardel_request_bad_user():
    os.environ['OARDO_USER'] = 'toto'
    config['WALLTIME_CHANGE_ENABLED'] = 'YES'
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='bad_user')
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id), '1:2:3'])
    print(result.output)
    assert result.exit_code == 3
    assert re.match(r'.*does not belong to you.*', result.output)

def test_oardel_request_not_running():
    os.environ['OARDO_USER'] = 'alice'
    config['WALLTIME_CHANGE_ENABLED'] = 'YES'

    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='alice')
    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id), '1:2:3'])
    print(result.output)
    assert result.exit_code == 3
    assert re.match(r'.*is not running.*', result.output)
    
def test_oardel_request(minimal_db_initialization):
    os.environ['OARDO_USER'] = 'alice'
    config['WALLTIME_CHANGE_ENABLED'] = 'YES'
    job_id = insert_running_jobs(1, user='alice')[0]

    runner = CliRunner()
    result = runner.invoke(cli, [str(job_id), '1:2:3'])

    walltime_change = db['WalltimeChange'].query.one()

    print(result.output)
    print(fake_notifications)
    print(walltime_change)
    
    assert result.exit_code == 0
    assert walltime_change.job_id == job_id
    assert walltime_change.pending == 3663
    assert walltime_change.force == 'NO'
    assert walltime_change.delay_next_jobs == 'NO'
    assert walltime_change.granted == 0
    assert walltime_change.granted_with_force == 0
    assert walltime_change.granted_with_delay_next_jobs == 0
    assert re.match(r'.*Accepted:.*', result.output)
    assert fake_notifications == ['Walltime']
