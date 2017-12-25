# coding: utf-8
import pytest

from oar.modules.node_change_state import (main, NodeChangeState)
from oar.lib import (db, Job, EventLog)
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
