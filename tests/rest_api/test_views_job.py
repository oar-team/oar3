# -*- coding: utf-8 -*-
import pytest
from .conftest import ordered

from flask import url_for
from oar.lib import (db, Job, FragJob)
from oar.kao.job import (insert_job, set_job_state)
from oar.kao.meta_sched import meta_schedule

# TODO test PAGINATION
# nodes / resources

def test_app_jobs_index(client):
    assert client.get(url_for('jobs.index')).status_code == 200

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_all(client):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    res = client.get(url_for('jobs.index'))
    print(res.json, len(res.json['items']))
    assert len(res.json['items']) == 1

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_details(client):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    res = client.get(url_for('jobs.index', detailed='details'))
    print(res.json, len(res.json['items']))
    assert res.json['items'][0]['type'] == 'PASSIVE'

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_table(client):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    res = client.get(url_for('jobs.index', detailed='table'))
    print(res.json, len(res.json['items']))
    assert res.json['items'][0]['type'] == 'PASSIVE'

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_one(client):
    """GET /jobs/show/<id>"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    res = client.get(url_for('jobs.show', job_id=job_id))
    print(res.json)
    assert res.json['type'] == 'PASSIVE'

    
@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_all_paginate(client):
    for i in range(10):
        insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    res1 = client.get(url_for('jobs.index', offset=0, limit=5))
    print(res1.json, len(res1.json['items']))
    res2 = client.get(url_for('jobs.index', offset=7, limit=5))
    print(res2.json, len(res2.json['items']))
    assert len(res1.json['items']) == 5
    assert len(res2.json['items']) == 3

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_get_one_details(client):
    """GET /jobs/show/<id>/details"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    meta_schedule('internal')
    res = client.get(url_for('jobs.show', job_id=job_id, detailed='details'))
    print(res.json)
    assert len(res.json['resources']) == 4

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_user(client):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='bob')
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='alice')
    res = client.get(url_for('jobs.index', user='bob'))
    print(res.json, len(res.json['items']))
    assert len(res.json['items']) == 1

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_state(client):
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='bob')
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='alice')
    set_job_state(job_id, 'Hold')
    res = client.get(url_for('jobs.index', state=['Waiting', 'Running']))
    print(res.json, len(res.json['items']))
    assert len(res.json['items']) == 1
    
@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_ids(client):
    job_id1 = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='bob')
    job_id2 = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='alice')
    res = client.get(url_for('jobs.index', ids='{}:{}'.format(job_id1, job_id2)))
    print(res.json, len(res.json['items']))
    assert len(res.json['items']) == 2
    
@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_job_post_forbidden(client):
    data = {'resource':[], 'command':'sleep "1"'}
    res = client.post(url_for('jobs.submit'), data=data)
    assert res.status_code == 403

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_job_post(client):
    data = {'resource':[], 'command':'sleep "1"'}
    res = client.post(url_for('jobs.submit'), data=data, headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    job_ids = db.query(Job.id).all()
    href = '/jobs/{}'.format(job_ids[0][0])
    assert ordered(res.json['links']) == ordered([{'rel': 'rel', 'href': href}])
    assert res.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_delete_1(client, monkeypatch):
    """POST /jobs/<id>/deletions/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    res = client.post(url_for('jobs.delete', job_id=job_id), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    fragjob_id = db.query(FragJob.job_id).filter(FragJob.job_id == job_id).one()
    assert fragjob_id[0] == job_id
    assert res.json['exit_status'] == 0
    
@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_delete_2(client, monkeypatch):
    """DELETE /jobs/<id>/deletions/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    res = client.delete(url_for('jobs.delete', job_id=job_id), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    fragjob_id = db.query(FragJob.job_id).filter(FragJob.job_id == job_id).one()
    assert fragjob_id[0] == job_id
    assert res.json['exit_status'] == 0

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_array_delete_1(client, monkeypatch):
    """POST /jobs/array/<id>/deletions/new"""
    array_id = 1
    for _ in range(5):
        insert_job(res=[(60, [('resource_id=4', '')])], properties='', user='bob',
                   array_id=array_id)
    res = client.post(url_for('jobs.delete',  array='array', job_id=array_id),
                      headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    fragjob_id = db.query(FragJob.job_id).all()
    assert len(fragjob_id) == 5
    assert res.json['exit_status'] == 0

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_array_delete_2(client, monkeypatch):
    """DELETE /jobs/array/<id>/deletions/new"""
    array_id = 1
    for _ in range(5):
        insert_job(res=[(60, [('resource_id=4', '')])], properties='', user='bob',
                   array_id=array_id)
    res = client.delete(url_for('jobs.delete',  array='array', job_id=array_id),
                      headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    fragjob_id = db.query(FragJob.job_id).all()
    assert len(fragjob_id) == 5
    assert res.json['exit_status'] == 0


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_ckeckpoint_1(client, monkeypatch):
    """POST /jobs/<id>/checkpoints/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    res = client.post(url_for('jobs.signal', job_id=job_id), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    # Can not checkpoint job is not running
    assert res.json['exit_status'] == 5

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_signal_1(client, monkeypatch):
    """POST /jobs/<id>/signal/<signal>"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    res = client.post(url_for('jobs.signal', job_id=job_id, signal=12), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    # Can not checkpoint job is not running
    assert res.json['exit_status'] == 5

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_ckeckpoint_2(client, monkeypatch):
    """POST /jobs/<id>/checkpoints/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    meta_schedule('internal')
    set_job_state(job_id, 'Running')
    res = client.post(url_for('jobs.signal', job_id=job_id), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    assert res.json['exit_status'] == 0

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_signal_2(client, monkeypatch):
    """POST /jobs/<id>/signal/<signal>"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    meta_schedule('internal')
    set_job_state(job_id, 'Running')
    res = client.post(url_for('jobs.signal', job_id=job_id, signal=12), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    assert res.json['exit_status'] == 0

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_hold_1(client, monkeypatch):
    """POST /jobs/<id>/holds/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    res = client.post(url_for('jobs.hold', job_id=job_id, hold='hold'), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    assert res.json['exit_status'] == 0

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_hold_2(client, monkeypatch):
    """POST /jobs/<id>/holds/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    meta_schedule('internal')
    set_job_state(job_id, 'Running')
    res = client.post(url_for('jobs.hold', job_id=job_id, hold='hold'), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    assert res.json['exit_status'] == 1
    
@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_rhold_user_not_allowed_1(client, monkeypatch):
    """POST /jobs/<id>/rholds/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    res = client.post(url_for('jobs.hold', job_id=job_id, hold='rhold'), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    assert res.json['exit_status'] == 1

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_rhold_2(client, monkeypatch):
    """POST /jobs/<id>/rholds/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    res = client.post(url_for('jobs.hold', job_id=job_id, hold='rhold'), headers={'X_REMOTE_IDENT': 'oar'})
    print(res.json)
    assert res.status_code == 200
    assert res.json['exit_status'] == 0

    
@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_resume_bad_nohold(client, monkeypatch):
    """POST /jobs/<id>/resumptions/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    res = client.post(url_for('jobs.resume', job_id=job_id), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    assert res.json['exit_status'] == 1


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_resume_not_allowed(client, monkeypatch):
    """POST /jobs/<id>/resumptions/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    set_job_state(job_id, 'Suspended')
    res = client.post(url_for('jobs.resume', job_id=job_id), headers={'X_REMOTE_IDENT': 'bob'})
    print(res.json)
    assert res.status_code == 200
    assert res.json['exit_status'] == 1  

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_resume(client, monkeypatch):
    """POST /jobs/<id>/resumptions/new"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    set_job_state(job_id, 'Suspended')
    res = client.post(url_for('jobs.resume', job_id=job_id), headers={'X_REMOTE_IDENT': 'oar'})
    print(res.json)
    assert res.status_code == 200
    assert res.json['exit_status'] == 0
    
#@pytest.mark.usefixtures("monkeypatch_tools")
#def test_app_jobs_get_one_resources(client, monkeypatch):
#    """GET /jobs/<id>/resources"""
#    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
#    meta_schedule('internal')
#    res = client.get(url_for('jobs.resources', job_id=job_id))
#    print(res.json, len(res.json['items']))
#    assert res.json['items'][0]['id'] == job_id
#    assert res.json['items'][0]['type'] == 'PASSIVE'

#TODO
#@app.args({'offset': Arg(int, default=0),
#           'limit': Arg(int),
#           'user': Arg(str),
#           'from': Arg(int, dest='start_time'),
#            'to': Arg(int, dest='stop_time'),
#           'state': Arg([str, ','], dest='states'),
#           'array': Arg(int, dest='array_id'),
#           'ids'
