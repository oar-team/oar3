# -*- coding: utf-8 -*-
import pytest
from flask import url_for
from oar.rest_api.app import create_app
from oar.rest_api.query import APIQuery

from oar.lib import db
from oar.lib.basequery  import BaseQuery
from oar.kao.job import (insert_job, set_job_state)

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj
@pytest.yield_fixture(scope='function')
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')
        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost" + str(int(i / 2)))
        yield

@pytest.fixture
def app(request):
    app = create_app()
    # force to use APIQuery needed when all tests are launched and previous ones have set BaseQuery
    db.sessionmaker.configure(query_cls=APIQuery)

    @request.addfinalizer
    def teardown():
        db.sessionmaker.configure(query_cls=BaseQuery)

    
    return app

def test_app_frontend_index(client):
    assert client.get(url_for('frontend.index')).status_code == 200

def test_app_resources_index(client):
    assert client.get(url_for('resources.index')).status_code == 200
    
def test_app_jobs_index(client):
    assert client.get(url_for('jobs.index')).status_code == 200

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_get_all(client):
    res = client.get(url_for('resources.index'))
    print(res.json, len(res.json['items']))
    assert res.status_code == 200
    assert len(res.json['items']) == 5    

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_all(client):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    res = client.get(url_for('jobs.index'))
    print(res.json, len(res.json['items']))
    assert res.status_code == 200
    assert len(res.json['items']) == 1

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
    assert  ordered(res.json['links'])==ordered([{'rel': 'rel', 'href': '/jobs/1'}])
    assert res.status_code == 200
