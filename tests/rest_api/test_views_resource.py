import pytest
#from .conftest import ordered

from flask import url_for
def test_app_resources_index(client):
    res = client.get(url_for('resources.index'))
    print(res.json)
    assert res.status_code == 200 and 'api_timestamp' in res.json

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_get_all(client):
    res = client.get(url_for('resources.index'))
    print(res.json, len(res.json['items']))
    assert res.status_code == 200
    assert len(res.json['items']) == 10    

@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_get_details(client):
    res = client.get(url_for('resources.index', detailed='details'))
    print(res.json, len(res.json['items']))
    assert res.status_code == 200
    assert len(res.json['items']) == 10
    assert 'suspended_jobs' in res.json['items'][0]
