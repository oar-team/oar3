# -*- coding: utf-8 -*-
import pytest
from flask import url_for
from oar.rest_api.app import create_app
from oar.rest_api.query import APIQuery

from oar.lib import db
from oar.kao.job import (insert_job, set_job_state)

@pytest.yield_fixture(scope='function')
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')
        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost" + str(int(i / 2)))
        yield

@pytest.fixture
def app():
    app = create_app()
    # force to use APIQuery needed when all tests are launched and previous ones have set BaseQuery
    db.sessionmaker.configure(query_cls=APIQuery)
    return app

def test_app_frontend_index(client):
    assert client.get(url_for('frontend.index')).status_code == 200

def test_app_resources_index(client):
    assert client.get(url_for('resources.index')).status_code == 200
    
def test_app_jobs_index(client):
    assert client.get(url_for('jobs.index')).status_code == 200

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_get_all(client):
    r = client.get(url_for('resources.index'))
    print(r.json, len(r.json['items']))
    assert r.status_code == 200
    assert len(r.json['items']) == 5    

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_all(client):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    r = client.get(url_for('jobs.index'))
    print(r.json, len(r.json['items']))
    assert r.status_code == 200
    assert len(r.json['items']) == 1
