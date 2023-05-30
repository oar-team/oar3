# -*- coding: utf-8 -*-
from tempfile import mkstemp

from oar import VERSION
from oar.api import API_VERSION


def test_app_frontend_index(client, setup_config):
    config, _, db = setup_config
    res = client.get("/")
    print(res.json)
    assert res.status_code == 200 and "api_timestamp" in res.json()


def test_app_frontend_version(client, setup_config):
    config, _, db = setup_config
    res = client.get("/version")
    print(res.json())
    assert res.status_code == 200 and "api_timestamp" in res.json()
    assert res.json()["oar_version"] == VERSION
    assert res.json()["api_version"] == API_VERSION

    assert "apilib_version" in res.json()


def test_app_frontend_whoami(client, setup_config):
    config, _, db = setup_config
    res = client.get("/whoami")
    print(res.json())
    assert res.status_code == 200 and "api_timestamp" in res.json()
    assert res.json()["authenticated_user"] is None


def test_app_frontend_timezone(client, setup_config):
    config, _, db = setup_config
    res = client.get("/timezone")
    print(res.json())
    assert res.status_code == 200 and "api_timestamp" in res.json()
    assert "api_timezone" in res.json()


def test_app_frontend_authentication(client, setup_config):
    config, _, db = setup_config
    _, htpasswd_filename = mkstemp()
    config["HTPASSWD_FILE"] = htpasswd_filename

    with open(htpasswd_filename, "w", encoding="utf-8") as f:
        f.write("user1:$apr1$yWaXLHPA$CeVYWXBqpPdN78e5FvbY3/")

    res = client.get(
        "/authentication", params={"basic_user": "user1", "basic_password": "user1"}
    )
    print(res.json())
    assert res.status_code == 200 and res.json()["basic authentication"] == "valid"


def test_app_frontend_authentication_wrong_passwd(client, setup_config):
    config, _, db = setup_config
    _, htpasswd_filename = mkstemp()
    config["HTPASSWD_FILE"] = htpasswd_filename

    with open(htpasswd_filename, "w", encoding="utf-8") as f:
        f.write("user1:$apr1$yWaXLHPA$CeVYWXBqpPdN78e5FvbY3/")

    res = client.get(
        "/authentication",
        params={"basic_user": "user1", "basic_password": "wrong password"},
    )
    print(res.json())
    assert res.status_code == 400
