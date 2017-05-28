import pytest

import json

from flask import url_for
from oar.lib import (db, Resource)
from oar.kao.job import (insert_job, set_job_state)
from oar.kao.meta_sched import meta_schedule


from flask import url_for
def test_app_resources_index(client):
    res = client.get(url_for('resources.index'))
    print(res.json)
    assert res.status_code == 200 and 'api_timestamp' in res.json

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_get_all(client):
    """GET /resources/"""
    res = client.get(url_for('resources.index'))
    print(res.json, len(res.json['items']))
    assert res.status_code == 200
    assert len(res.json['items']) == 10    

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_get_details(client):
    """GET /resources/details"""
    res = client.get(url_for('resources.index', detailed='details'))
    print(res.json, len(res.json['items']))
    assert res.status_code == 200
    assert len(res.json['items']) == 10
    assert 'suspended_jobs' in res.json['items'][0]

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_get_details_paginate(client):
    """GET /resources/details w/ pagination"""
    res1 = client.get(url_for('resources.index', detailed='details', offset=0, limit=5))
    res2 = client.get(url_for('resources.index', detailed='details', offset=7, limit=5))
    assert len(res1.json['items']) == 5
    assert len(res2.json['items']) == 3
    assert 'suspended_jobs' in res1.json['items'][0]
    assert 'suspended_jobs' in res2.json['items'][0]

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_nodes(client):
    """GET /resources/nodes/<network_address>"""
    res = client.get(url_for('resources.index', network_address='localhost2'))
    print(res.json)
    assert len(res.json['items']) == 2
    assert res.status_code == 200

@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_resources_jobs(client, monkeypatch):
    """GET /resources/<id>/jobs"""
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user="bob")
    meta_schedule('internal')
    set_job_state(job_id, 'Running')
    db.commit()
    res = client.get(url_for('resources.jobs', resource_id=3))
    print(res.json)
    assert res.status_code == 200
    assert res.json['items'][0]['id'] == job_id



@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_create_resource(client):
    """POST /resources"""
    props = json.dumps({'cpu':2, 'core':3})

    res = client.post(url_for('resources.create', hostname='akira', properties=props),\
                      headers={'X_REMOTE_IDENT': 'oar'})

    r = db.query(Resource.network_address, Resource.cpu, Resource.core)\
          .filter(Resource.network_address == 'akira').one()
    
    print(res.json)
    assert res.status_code == 200
    assert r == ('akira', 2, 3)
    
