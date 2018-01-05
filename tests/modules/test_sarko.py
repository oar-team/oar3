# coding: utf-8
import pytest
from oar.modules.sarko import Sarko

from oar.lib import (db, config, Job, Resource, AssignedResource, EventLog)
from oar.lib.job_handling import insert_job

import oar.lib.tools  # for monkeypatching

fake_date = 0

def set_fake_date(date):
    global fake_date
    fake_date = date

def fake_get_date():
    return fake_date

def fake_signal_oarexec(host, job_id, signal_name, detach, openssh_cmd):
    return ''

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'get_date', fake_get_date)
    monkeypatch.setattr(oar.lib.tools, 'signal_oarexec', fake_signal_oarexec)

def assign_resources(job_id):
    db.query(Job).filter(Job.id == job_id)\
                 .update({Job.assigned_moldable_job: job_id}, synchronize_session=False)
    resources = db.query(Resource).all()
    for r in resources[:4]:
        AssignedResource.create(moldable_id=job_id, resource_id=r.id)
        
def test_sarko_void():
    sarko = Sarko()
    sarko.run()
    print(sarko.guilty_found)
    assert sarko.guilty_found == 0

    
def test_sarko_job_walltime_reached():
    """ date > (start_time + max_time): """
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='', state='Running')
    assign_resources(job_id)
    
    set_fake_date(100)

    sarko = Sarko()
    sarko.run()
    
    # Reset date
    set_fake_date(0)
    
    print(sarko.guilty_found)
    assert sarko.guilty_found == 1

def test_sarko_job_to_checkpoint():
    """(date >= (start_time + max_time - job.checkpoint))"""
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='', state='Running',
                        checkpoint=30)
    assign_resources(job_id)
    
    set_fake_date(45) # > 0+60 - 30   
    sarko = Sarko()
    sarko.run()
    # Reset date
    set_fake_date(0)

    event = db.query(EventLog).filter(EventLog.type=='CHECKPOINT_SUCCESSFULL').first()
    
    print(sarko.guilty_found)
    assert sarko.guilty_found == 0
    assert event.job_id == job_id
