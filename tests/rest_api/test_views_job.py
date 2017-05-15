# -*- coding: utf-8 -*-
import pytest
from .conftest import ordered

from flask import url_for
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

    
#@pytest.mark.usefixtures("minimal_db_initialization")
#def test_app_jobs_get_all_paginate(client):
#    for i in range(10):
#        insert_job(res=[(60, [('resource_id=4', "")])], properties="")
#    res = client.get(url_for('jobs.index'))
#    print(res.json, len(res.json['items']))
#    assert len(res.json['items']) == 1
#

    

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_get_one_details(client, monkeypatch):
    """GET /jobs/show/<id>/details"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    meta_schedule('internal')
    res = client.get(url_for('jobs.show', job_id=job_id, detailed='details'))
    print(res.json)
    assert len(res.json['resources']) == 4

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
    assert ordered(res.json['links'])==ordered([{'rel': 'rel', 'href': '/jobs/1'}])
    assert res.status_code == 200

#@pytest.mark.usefixtures("minimal_db_initialization")
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
