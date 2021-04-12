# -*- coding: utf-8 -*-
from tempfile import mkstemp

import pytest
from flask import url_for

from oar.lib import config


def test_app_frontend_index(client):
    res = client.get(url_for("frontend.index"))
    print(res.json)
    assert res.status_code == 200 and "api_timestamp" in res.json
    assert "links" in res.json


def test_app_frontend_version(client):
    res = client.get(url_for("frontend.version"))
    print(res.json)
    assert res.status_code == 200 and "api_timestamp" in res.json
    assert "apilib_version" in res.json


def test_app_frontend_whoami(client):
    res = client.get(url_for("frontend.whoami"))
    print(res.json)
    assert res.status_code == 200 and "api_timestamp" in res.json
    assert res.json["authenticated_user"] == None


def test_app_frontend_timezone(client):
    res = client.get(url_for("frontend.timezone"))
    print(res.json)
    assert res.status_code == 200 and "api_timestamp" in res.json
    assert "api_timezone" in res.json


def test_app_frontend_authentication(client):
    _, htpasswd_filename = mkstemp()
    config["HTPASSWD_FILE"] = htpasswd_filename

    with open(htpasswd_filename, "w", encoding="utf-8") as f:
        f.write("user1:$apr1$yWaXLHPA$CeVYWXBqpPdN78e5FvbY3/")

    res = client.get(
        url_for("frontend.authentication", basic_user="user1", basic_password="user1")
    )
    print(res.json)
    assert res.status_code == 200 and res.json["basic authentication"] == "valid"


def test_app_frontend_authentication_wrong_passwd(client):
    _, htpasswd_filename = mkstemp()
    config["HTPASSWD_FILE"] = htpasswd_filename

    with open(htpasswd_filename, "w", encoding="utf-8") as f:
        f.write("user1:$apr1$yWaXLHPA$CeVYWXBqpPdN78e5FvbY3/")

    res = client.get(
        url_for(
            "frontend.authentication", basic_user="user1", basic_password="wrong passwd"
        )
    )
    print(res.json)
    assert res.status_code == 400
