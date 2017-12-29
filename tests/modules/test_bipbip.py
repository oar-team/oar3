# coding: utf-8
import pytest

from oar.modules.bipbip import BipBip

from oar.lib import (db, config, Job, Challenge, Resource, AssignedResource)
from oar.lib.job_handling import insert_job

import oar.lib.tools  # for monkeypatching

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield

def fake_pingchecker(hosts):
    return []

def fake_launch_oarexec(cmt,data, oarexec_files):
    return True

def fake_manage_remote_commands(hosts, data_str, manage_file, action, ssh_command, taktuk_cmd=None):
    return (1, [])

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: len(x))
    monkeypatch.setattr(oar.lib.tools, 'pingchecker', fake_pingchecker)
    monkeypatch.setattr(oar.lib.tools, 'notify_interactif_user', lambda x,y: None)
    monkeypatch.setattr(oar.lib.tools, 'launch_oarexec', fake_launch_oarexec)
    monkeypatch.setattr(oar.lib.tools, 'manage_remote_commands', fake_manage_remote_commands)

        
def test_bipbip_void():
    bipbip = BipBip(None)
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1

def test_bipbip_simple():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='')
    Challenge.create(job_id=job_id, challenge='foo1', ssh_private_key='foo2', ssh_public_key='foo2')
    
    # Bipbip needs a job id
    bipbip = BipBip([job_id])
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1

def _test_bipbip_toLaunch(noop=False):

    types = ['noop'] if noop else []
    
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='', command='yop',
                        state='toLaunch', stdout_file='poy', stderr_file='yop', types=types)
    db.query(Job).update({Job.assigned_moldable_job: job_id}, synchronize_session=False)
    Challenge.create(job_id=job_id, challenge='foo1', ssh_private_key='foo2', ssh_public_key='foo2')

    resources = db.query(Resource).all()
    for r in resources[:4]:
        AssignedResource.create(moldable_id=job_id, resource_id=r.id)

    config['SERVER_HOSTNAME'] = 'localhost'
    config['DETACH_JOB_FROM_SERVER'] = 'localhost'
    
    # Bipbip needs a job id
    bipbip = BipBip([job_id])
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0

def test_bipbip_toLaunch():
    _test_bipbip_toLaunch()
    
def test_bipbip_toLaunch_noop():
    _test_bipbip_toLaunch(True)
    
