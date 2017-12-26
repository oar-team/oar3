# coding: utf-8
import pytest
import os

from oar.modules.node_change_state import (main, NodeChangeState)
from oar.lib import (db, Job, EventLog, Challenge)
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

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: len(x))

def test_node_change_state_main():
    exit_code = main()
    print(exit_code)
    assert exit_code == 0

def test_node_change_state_void():
    node_change_state = NodeChangeState()
    node_change_state.run()
    print(node_change_state.exit_code)
    assert node_change_state.exit_code == 0

def test_node_change_state_error():
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    ev = EventLog.create(to_check='YES', job_id=job_id, type='EXTERMINATE_JOB')
    node_change_state = NodeChangeState()
    node_change_state.run()
    job = db.query(Job).filter(Job.id==job_id).first()
    print(node_change_state.exit_code)    
    assert node_change_state.exit_code == 0
    assert job.state == 'Error'

def test_node_change_state_job_idempotent_exitcode_25344():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties="", exit_code=25344, types=['idempotent','timesharing=*,*'], start_time=10, stop_time=100)
    ev = EventLog.create(to_check='YES', job_id=job_id, type='SWITCH_INTO_TERMINATE_STATE')
    os.environ['OARDO_USER'] = 'oar'
    node_change_state = NodeChangeState()
    node_change_state.run()
    job = db.query(Job).filter(Job.id==job_id).first()
    print(node_change_state.exit_code)    
    assert node_change_state.exit_code == 0
    assert job.state == 'Terminated'
    

def test_node_change_state_job_check_toresubmit():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='')
    ev = EventLog.create(to_check='YES', job_id=job_id, type='SERVER_PROLOGUE_TIMEOUT')
    os.environ['OARDO_USER'] = 'oar'
    Challenge.create(job_id=job_id, challenge='foo1', ssh_private_key='foo2', ssh_public_key='foo2')

    node_change_state = NodeChangeState()
    node_change_state.run()

    event = db.query(EventLog).filter(EventLog.type=='RESUBMIT_JOB_AUTOMATICALLY').first()
    print(node_change_state.exit_code)    
    assert node_change_state.exit_code == 0
    assert event.job_id == job_id
