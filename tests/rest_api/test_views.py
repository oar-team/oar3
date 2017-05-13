# -*- coding: utf-8 -*-
import pytest
from flask import url_for

def test_app_frontend_index(client):
    res = client.get(url_for('frontend.index'))
    print(res.json)
    assert res.status_code == 200 and 'api_timestamp' in res.json
    assert 'links' in res.json

def test_app_frontend_version(client):
    res = client.get(url_for('frontend.version'))
    print(res.json)
    assert res.status_code == 200 and 'api_timestamp' in res.json
    assert 'apilib_version' in res.json

def test_app_frontend_whoami(client):
    res = client.get(url_for('frontend.whoami'))
    print(res.json)
    assert res.status_code == 200 and 'api_timestamp' in res.json
    assert res.json['authenticated_user'] == None
                    
def test_app_fronend_timezone(client):
    res = client.get(url_for('frontend.timezone'))
    print(res.json)
    assert res.status_code == 200 and 'api_timestamp' in res.json
    assert 'api_timezone' in res.json
