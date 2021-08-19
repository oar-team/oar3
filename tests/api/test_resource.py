import pytest

from oar.lib import db


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization():
    with db.session(ephemeral=True):
        db["Queue"].create(
            name="default", priority=3, scheduler_policy="kamelot", state="Active"
        )
        # add some resources
        for i in range(10):
            db["Resource"].create(network_address="localhost" + str(int(i / 2)))
        yield


@pytest.mark.usefixtures("minimal_db_initialization")
def test_get_all(client):
    response = client.get("/resources/")
    assert response.status_code == 200
    print(response.json())
    assert len(response.json()) == 10


@pytest.mark.usefixtures("minimal_db_initialization")
def test_get_paginate(client):
    params = {"offset": "0", "limit": "5"}
    response = client.get("/resources/", params=params)
    assert response.status_code == 200
    print(response.json())
    assert len(response.json()) == 5


@pytest.mark.usefixtures("minimal_db_initialization")
def test_get_one(client):
    id_to_get = 21
    response = client.get("/resources/{}".format(id_to_get))
    assert response.status_code == 200
    assert response.json()["id"] == id_to_get
