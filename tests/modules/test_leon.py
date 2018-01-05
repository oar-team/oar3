# coding: utf-8
import pytest
from oar.modules.leon import Leon

from oar.lib.job_handling import insert_job

from oar.lib import (db, config, FragJob, EventLog)
import oar.lib.tools  # for monkeypatching

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: True)

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost")
        db['Queue'].create(name='default')
        yield

def test_leon_void():
    # Leon needs of job id
    leon = Leon()
    leon.run()
    print(leon.exit_code)
    assert leon.exit_code == 0

def test_leon_simple():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='')
    leon = Leon(str(job_id))
    leon.run()
    print(leon.exit_code)
    assert leon.exit_code == 0
    
def test_leon_exterminate():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='')

    FragJob.create(job_id=job_id, state='LEON_EXTERMINATE')
    
    leon = Leon(str(job_id))
    leon.run()

    event = db.query(EventLog).filter(EventLog.type=='EXTERMINATE_JOB').first()
    
    print(leon.exit_code)
    assert leon.exit_code == 0
    assert event.job_id == job_id
