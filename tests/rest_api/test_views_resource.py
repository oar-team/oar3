import pytest
#from .conftest import ordered

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
