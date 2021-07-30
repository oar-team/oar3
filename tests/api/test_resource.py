import pytest

# from oar.api.routers.resource import get_db
from fastapi.testclient import TestClient

from oar.api.app import app
from oar.lib import db

client = TestClient(app)


@pytest.fixture(scope="function")
def minimal_db_initialization():
    with db.session(ephemeral=True):
        db["Queue"].create(
            name="default", priority=3, scheduler_policy="kamelot", state="Active"
        )
        # add some resources
        for i in range(10):
            db["Resource"].create(network_address="localhost" + str(int(i / 2)))
        yield


# def override_get_db():
#     try:
#         session = db.session
#         import pdb; pdb.set_trace()
#         yield session
#     finally:
#         db.close()
# app.dependency_overrides[get_db] = override_get_db


@pytest.mark.usefixtures("minimal_db_initialization")
def test_get_all():
    # resources = db.query(Resource).all()
    response = client.get("/resources")
    assert response.status_code == 200
    print(response.json())
    assert len(response.json()) == 10


@pytest.mark.usefixtures("minimal_db_initialization")
def test_get_paginate():
    params = {"offset": "0", "limit": "5"}
    response = client.get("/resources", params=params)
    assert response.status_code == 200
    print(response.json())
    assert len(response.json()) == 5


@pytest.mark.usefixtures("minimal_db_initialization")
def test_get_one():
    id_to_get = 1
    response = client.get("/resources/{}".format(id_to_get))
    assert response.status_code == 200
    assert response.json()["id"] == id_to_get
    print(response.json())
