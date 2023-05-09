import json

import pytest

from oar.kao.meta_sched import meta_schedule
from oar.lib.job_handling import insert_job, set_job_state
from oar.lib.models import FragJob, Job, Resource


def test_get_all(client, minimal_db_initialization, setup_config):
    config, _, db = setup_config
    print("hello!")
    response = client.get("/resources/")
    print(response)
    print(response.json())
    assert response.status_code == 200
    assert len(response.json()["items"]) == 10


def test_get_paginate(client, minimal_db_initialization, setup_config):
    config, _, db = setup_config
    params = {"offset": "0", "limit": "5"}
    response = client.get("/resources/", params=params)
    assert response.status_code == 200
    print(response.json())


def test_app_resources_get_details_paginate(
    client, minimal_db_initialization, setup_config
):
    """GET /resources/details w/ pagination"""
    config, _, db = setup_config
    res1 = client.get("/resources/", params={"offset": 0, "limit": 5, "detailed": True})
    res2 = client.get("/resources/", params={"detailed": True, "offset": 7, "limit": 5})
    assert len(res1.json()["items"]) == 5
    assert len(res2.json()["items"]) == 3
    assert "suspended_jobs" in res1.json()["items"][0]
    assert "suspended_jobs" in res2.json()["items"][0]


def test_app_resources_nodes(client, minimal_db_initialization, setup_config):
    """GET /resources/nodes/<network_address>"""
    config, _, db = setup_config
    res = client.get("/resources", params={"network_address": "localhost2"})
    print(res.json())
    assert len(res.json()["items"]) == 2
    assert res.status_code == 200


@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_resources_jobs(
    client, minimal_db_initialization, monkeypatch, setup_config
):
    """GET /resources/<id>/jobs"""
    config, _, db = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    meta_schedule(minimal_db_initialization, config, "internal")
    set_job_state(minimal_db_initialization, config, job_id, "Running")
    minimal_db_initialization.commit()
    first_id = minimal_db_initialization.query(Resource).first().id
    res = client.get("/resources/{resource_id}/jobs".format(resource_id=first_id + 3))
    print(res.json())
    assert res.status_code == 200
    assert res.json()["items"][0]["id"] == job_id


def test_app_create_resource(client, minimal_db_initialization, setup_config):
    """POST /resources"""
    config, _, db = setup_config
    props = json.dumps({"cpu": 2, "core": 3})

    res = client.post(
        "/resources/",
        params={"hostname": "akira", "properties": props},
        headers={"x-remote-ident": "oar"},
    )

    r = (
        minimal_db_initialization.query(
            Resource.network_address, Resource.cpu, Resource.core
        )
        .filter(Resource.network_address == "akira")
        .one()
    )

    print(res.json)
    assert res.status_code == 200
    assert r == ("akira", 2, 3)


@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_resource_state(client, minimal_db_initialization, setup_config):
    """POST /resources/<id>/state"""
    config, _, db = setup_config
    minimal_db_initialization.commit()
    first_id = minimal_db_initialization.query(Resource).first().id
    r_id = first_id + 4
    r1 = (
        minimal_db_initialization.query(Resource.state)
        .filter(Resource.id == r_id)
        .one()
    )
    print(r1)
    res = client.post(
        "/resources/{resource_id}/state".format(resource_id=r_id),
        json={"state": "Dead"},
        headers={"x-remote-ident": "oar"},
    )
    print(res)

    r2 = (
        minimal_db_initialization.query(Resource.state)
        .filter(Resource.id == r_id)
        .one()
    )
    print(r2)
    print(res.json())
    assert r1 == ("Alive",) and r2 == ("Dead",)
    assert res.status_code == 200


def test_app_resource_delete(client, minimal_db_initialization, setup_config):
    """DELETE /resources/<id>"""
    config, _, db = setup_config
    Resource.create(
        minimal_db_initialization, network_address="localhost", state="Dead"
    )
    nb_res1 = len(minimal_db_initialization.query(Resource).all())
    first_id = minimal_db_initialization.query(Resource).first().id
    res = client.delete(
        "/resources/{}".format(first_id + nb_res1 - 1),
        headers={"x-remote-ident": "oar"},
    )
    nb_res2 = len(minimal_db_initialization.query(Resource).all())
    assert nb_res1 == 11
    assert nb_res2 == 10
    assert res.status_code == 200


@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_busy_resources(
    client, minimal_db_initialization, monkeypatch, setup_config
):
    """GET /resources/used"""
    config, _, db = setup_config
    job_id0 = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    job_id1 = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        properties="",
        user="bob",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        properties="",
        user="bob",
    )
    meta_schedule(minimal_db_initialization, config, "internal")
    set_job_state(minimal_db_initialization, config, job_id0, "Running")
    set_job_state(minimal_db_initialization, config, job_id1, "Error")
    minimal_db_initialization.commit()

    res = client.get("/resources/busy")
    print(res.json())
    assert res.status_code == 200
    assert res.json()["busy"] == 6
