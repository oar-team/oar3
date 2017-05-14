# -*- coding: utf-8 -*-
import pytest
from .conftest import ordered

from flask import url_for
from oar.kao.job import (insert_job, set_job_state)


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
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    res = client.get(url_for('jobs.index', job_id=job_id))
    print(res.json, len(res.json['items']))
    assert res.json['items'][0]['id'] == job_id
    
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


