# coding: utf-8
import pytest
from oar.modules.leon import Leon

from oar.lib.job_handling import insert_job

from oar.lib import (db, config, FragJob, EventLog, Job)
import oar.lib.tools  # for monkeypatching

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: True)
    monkeypatch.setattr(oar.lib.tools, 'notify_tcp_socket', lambda x,y,z: True)

def test_leon_void():
    # Leon needs of job id
    leon = Leon()
    leon.run()
    print(leon.exit_code)
    assert leon.exit_code == 0

def test_leon_simple():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='')
    leon = Leon([str(job_id)])
    leon.run()
    print(leon.exit_code)
    assert leon.exit_code == 0
    
def test_leon_simple_not_job_id_int():
    leon = Leon('zorglub')
    leon.run()
    print(leon.exit_code)
    assert leon.exit_code == 1
      
def test_leon_exterminate():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='')

    FragJob.create(job_id=job_id, state='LEON_EXTERMINATE')
    print('job_id:' + str(job_id))
    
    leon = Leon([str(job_id)])
    leon.run()

    event = db.query(EventLog).filter(EventLog.type=='EXTERMINATE_JOB')\
                              .filter(EventLog.job_id==job_id).first()

    for e in db.query(EventLog).all():
        print(EventLog.type, str(EventLog.job_id))

    print(leon.exit_code)
    assert leon.exit_code == 0
    assert event.job_id == job_id

def test_leon_get_jobs_to_kill_waiting():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='', state='Waiting')

    FragJob.create(job_id=job_id, state='LEON')
    
    leon = Leon()
    leon.run()

    job = db.query(Job).filter(Job.id == job_id).first()
    
    print(leon.exit_code)
    assert leon.exit_code == 1
    assert job.state == 'Error'   
