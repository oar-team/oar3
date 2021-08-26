import json

import pytest

from oar.kao.meta_sched import meta_schedule
from oar.lib import Resource, db
from oar.lib.job_handling import insert_job, set_job_state


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
    assert len(response.json()["items"]) == 10


@pytest.mark.usefixtures("minimal_db_initialization")
def test_get_paginate(client):
    params = {"offset": "0", "limit": "5"}
    response = client.get("/resources/", params=params)
    assert response.status_code == 200
    print(response.json())


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_get_details_paginate(client):
    """GET /resources/details w/ pagination"""
    res1 = client.get("/resources/", params={"offset": 0, "limit": 5, "detailed": True})
    res2 = client.get("/resources/", params={"detailed": True, "offset": 7, "limit": 5})
    assert len(res1.json()["items"]) == 5
    assert len(res2.json()["items"]) == 3
    assert "suspended_jobs" in res1.json()["items"][0]
    assert "suspended_jobs" in res2.json()["items"][0]


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resources_nodes(client):
    """GET /resources/nodes/<network_address>"""
    res = client.get("/resources", params={"network_address": "localhost2"})
    print(res.json())
    assert len(res.json()["items"]) == 2
    assert res.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_resources_jobs(client, monkeypatch):
    """GET /resources/<id>/jobs"""
    job_id = insert_job(res=[(60, [("resource_id=4", "")])], properties="", user="bob")
    meta_schedule("internal")
    set_job_state(job_id, "Running")
    db.commit()
    first_id = db.query(Resource).first().id
    res = client.get("/resources/{resource_id}/jobs".format(resource_id=first_id + 3))
    print(res.json())
    assert res.status_code == 200
    assert res.json()["items"][0]["id"] == job_id


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_create_resource(client):
    """POST /resources"""
    props = json.dumps({"cpu": 2, "core": 3})

    res = client.post(
        "/resources/",
        params={"hostname": "akira", "properties": props},
        headers={"X_REMOTE_IDENT": "oar"},
    )

    r = (
        db.query(Resource.network_address, Resource.cpu, Resource.core)
        .filter(Resource.network_address == "akira")
        .one()
    )

    print(res.json)
    assert res.status_code == 200
    assert r == ("akira", 2, 3)


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_resource_state(client):
    """POST /resources/<id>/state"""
    db.commit()
    first_id = db.query(Resource).first().id
    r_id = first_id + 4
    r1 = db.query(Resource.state).filter(Resource.id == r_id).one()
    print(r1)
    res = client.post(
        "/resources/{resource_id}/state".format(resource_id=r_id),
        json={"state": "Dead"},
        headers={"X_REMOTE_IDENT": "oar"},
    )
    print(res)

    r2 = db.query(Resource.state).filter(Resource.id == r_id).one()
    print(r2)
    print(res.json())
    assert r1 == ("Alive",) and r2 == ("Dead",)
    assert res.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_resource_delete(client):
    """DELETE /resources/<id>"""
    db["Resource"].create(network_address="localhost", state="Dead")
    nb_res1 = len(db.query(Resource).all())
    first_id = db.query(Resource).first().id
    res = client.delete(
        "/resources/{}".format(first_id + nb_res1 - 1),
        headers={"X_REMOTE_IDENT": "oar"},
    )
    nb_res2 = len(db.query(Resource).all())
    assert nb_res1 == 11
    assert nb_res2 == 10
    assert res.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_busy_resources(client, monkeypatch):
    """GET /resources/used"""
    job_id0 = insert_job(res=[(60, [("resource_id=4", "")])], properties="", user="bob")
    job_id1 = insert_job(res=[(60, [("resource_id=2", "")])], properties="", user="bob")
    insert_job(res=[(60, [("resource_id=2", "")])], properties="", user="bob")
    meta_schedule("internal")
    set_job_state(job_id0, "Running")
    set_job_state(job_id1, "Error")
    db.commit()

    res = client.get("/resources/busy")
    print(res.json())
    assert res.status_code == 200
    assert res.json()["busy"] == 6
