# coding: utf-8
import pytest
import os

from oar.modules.node_change_state import (main, NodeChangeState)
from oar.lib import (db, config, Job, EventLog, Challenge, Resource,
                     AssignedResource)
from oar.lib.job_handling import insert_job

import oar.lib.tools  # for monkeypatching

def fake_manage_remote_commands(hosts, data_str, manage_file, action, ssh_command, taktuk_cmd=None):
    return (1, [])

def fake_exec_with_timeout(args, timeout):
    return ''

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: True)
    monkeypatch.setattr(oar.lib.tools, 'manage_remote_commands', fake_manage_remote_commands)
    monkeypatch.setattr(oar.lib.tools, 'exec_with_timeout', fake_exec_with_timeout)
    
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

def test_node_change_state_job_suspend_resume():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='', state='Running')
    db.query(Job).update({Job.assigned_moldable_job: job_id}, synchronize_session=False)
    ev = EventLog.create(to_check='YES', job_id=job_id, type='HOLD_WAITING_JOB')
    #os.environ['OARDO_USER'] = 'oar'
    #Challenge.create(job_id=job_id, challenge='foo1', ssh_private_key='foo2', ssh_public_key='foo2')

    config['JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD'] = 'core'
    config['SUSPEND_RESUME_FILE'] = '/tmp/fake_suspend_resume'
    config['JUST_AFTER_SUSPEND_EXEC_FILE'] = '/tmp/fake_admin_script'
    config['SUSPEND_RESUME_SCRIPT_TIMEOUT'] = 60

    resources = db.query(Resource).all()
    for r in resources[:4]:
        AssignedResource.create(moldable_id=job_id, resource_id=r.id)

    node_change_state = NodeChangeState()
    node_change_state.run()

    #event = db.query(EventLog).filter(EventLog.type=='RESUBMIT_JOB_AUTOMATICALLY').first()
    print(node_change_state.exit_code)    
    assert node_change_state.exit_code == 0
    #assert event.job_id == job_id

    
