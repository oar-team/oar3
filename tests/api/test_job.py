# -*- coding: utf-8 -*-
import pytest

from oar.api.url_utils import replace_query_params
from oar.kao.meta_sched import meta_schedule
from oar.lib.job_handling import insert_job, set_job_state
from oar.lib.models import FragJob, Job


@pytest.mark.usefixtures("minimal_db_initialization")
def test_jobs_index(client, minimal_db_initialization):
    response = client.get("/jobs")
    assert response.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
def test_jobs_get_all(client, minimal_db_initialization):
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    response = client.get("/jobs")
    assert response.status_code == 200
    print(response.json())
    assert len(response.json()["items"]) == 1


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_details(client, minimal_db_initialization):
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    res = client.get("/jobs?details=details")

    print(res.json(), len(res.json()))
    assert res.json()["items"][0]["type"] == "PASSIVE"


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_table(client, minimal_db_initialization):
    insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    res = client.get("/jobs?details=table")
    assert res.status_code == 200
    print(res.json(), len(res.json()["items"]))
    assert res.json()["items"][0]["type"] == "PASSIVE"


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_one(client, minimal_db_initialization):
    """GET /jobs/<id>"""
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    res = client.get("/jobs/{}".format(job_id))
    assert res.status_code == 200
    print(res.json())
    assert res.json()["type"] == "PASSIVE"


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_all_paginate(client, minimal_db_initialization):
    for i in range(10):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
        )
    # minimal_db_initialization.commit()

    res1 = client.get("/jobs", params={"offset": 0, "limit": 5})
    print(res1.json(), len(res1.json()["items"]))
    print(res1._content)
    res2 = client.get("/jobs", params={"offset": 7, "limit": 5})
    print(res2._content)
    print(res2.json(), len(res2.json()["items"]))

    assert len(res1.json()["items"]) == 5
    assert len(res2.json()["items"]) == 3


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_get_one_details(client, minimal_db_initialization):
    """GET /jobs/<id>?details=true"""
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    meta_schedule("internal")
    res = client.get("/jobs/{}?details=true".format(job_id))
    print("json res:", res.json())
    assert len(res.json()["resources"]) == 4


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_get_resources(client, minimal_db_initialization):
    """GET /jobs/<id>/resources"""
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    meta_schedule("internal")
    res = client.get("/jobs/{}/resources".format(job_id))
    assert len(res.json()["items"]) == 4


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_user(client, minimal_db_initialization):
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="alice",
    )

    res = client.get("/jobs", params={"user": "bob"})
    print(res.json(), len(res.json()["items"]))
    assert len(res.json()["items"]) == 1


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_state(client, minimal_db_initialization):
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    set_job_state(job_id, "Hold")

    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="alice",
    )
    url = replace_query_params("/jobs", {"states": ["Waiting", "Running"]})
    print("url", url)
    res = client.get(url)

    print(res.json())
    assert len(res.json()["items"]) == 1


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_ids(client, minimal_db_initialization):
    job_id1 = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    job_id2 = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="alice",
    )
    # res = client.get(url_for("jobs.index", ids="{}:{}".format(job_id1, job_id2)))
    res = client.get("/jobs", params={"ids": "{}:{}".format(job_id1, job_id2)})

    print(res.json(), len(res.json()["items"]))
    assert len(res.json()["items"]) == 2


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_jobs_get_array(client, minimal_db_initialization):
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        state="Terminated",
        properties="",
        array_id=3,
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        array_id=3,
    )
    res = client.get(replace_query_params("/jobs", params={"array": 3}))
    print(res)
    print(res.json(), len(res.json()["items"]))
    assert len(res.json()["items"]) == 2


@pytest.mark.skip(reason="debug pending")
@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_get_from_to_ar(client, minimal_db_initialization):
    t0 = get_date()  # noqa
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        reservation="toSchedule",
        start_time=t0 + 10,
        info_type="localhost:4242",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        reservation="toSchedule",
        start_time=t0 + 70,
        info_type="localhost:4242",
    )
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        reservation="toSchedule",
        start_time=t0 + 200,
        info_type="localhost:4242",
    )
    meta_schedule("internal")
    res = client.get(
        "/jobs/details", params={"start_time": t0 + 50, "stop_time": t0 + 70}
    )
    print(res.json(), len(res.json()["items"]))
    assert len(res.json()["items"]) == 2


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_job_post_forbidden(client, minimal_db_initialization):
    data = {"resource": [], "command": 'sleep "1"'}
    url = replace_query_params("/jobs/", params=data)
    res = client.post(url)
    print(res.__dict__)
    assert res.status_code == 403


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_job_post(client, minimal_db_initialization):
    data = {"resource": [], "command": 'sleep "1"'}

    res = client.post("/jobs/", json=data, headers={"x-remote-ident": "bob"})

    job_ids = db.query(Job.id).all()
    print(res.json())
    assert job_ids != []
    assert res.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_delete_1(client, monkeypatch, minimal_db_initialization):
    """POST /jobs/<id>/deletions/new"""

    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    res = client.post(
        "/jobs/{}/deletions/new".format(job_id), headers={"x-remote-ident": "bob"}
    )

    print(res.json())
    assert res.status_code == 200
    fragjob_id = db.query(FragJob.job_id).filter(FragJob.job_id == job_id).one()
    assert fragjob_id[0] == job_id
    print(res.json())
    assert res.json()["exit_status"] == 0


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_delete_2(client, monkeypatch, minimal_db_initialization):
    """DELETE /jobs/<id>/deletions/new"""

    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    res = client.delete("/jobs/{}".format(job_id), headers={"x-remote-ident": "bob"})
    print(res.json())
    assert res.status_code == 200
    fragjob_id = db.query(FragJob.job_id).filter(FragJob.job_id == job_id).one()
    assert fragjob_id[0] == job_id
    assert res.json()["exit_status"] == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_array_delete_1(client, minimal_db_initialization):
    """POST /jobs/array/<id>/deletions/new"""
    array_id = 1
    for _ in range(5):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
            user="bob",
            array_id=array_id,
        )
    url = replace_query_params(
        "/jobs/{job_id}/deletions/new".format(job_id=1),
        params={"array": True},
    )
    res = client.post(
        url,
        headers={"x-remote-ident": "bob"},
    )

    assert res.status_code == 200
    fragjob_id = db.query(FragJob.job_id).all()
    assert len(fragjob_id) == 5
    assert res.json()["exit_status"] == 0


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_array_delete_2(client, monkeypatch, minimal_db_initialization):
    """DELETE /jobs/array/<id>/deletions/new"""
    array_id = 1
    for _ in range(5):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
            user="bob",
            array_id=array_id,
        )
    res = client.delete(
        replace_query_params(
            "jobs/{job_id}".format(job_id=array_id), params={"array": True}
        ),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    fragjob_id = db.query(FragJob.job_id).all()
    assert len(fragjob_id) == 5
    assert res.json()["exit_status"] == 0


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_ckeckpoint_1(client, monkeypatch, minimal_db_initialization):
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    res = client.post(
        "/jobs/{job_id}/checkpoints/new".format(job_id=job_id),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    # Can not checkpoint job is not running
    assert res.json()["exit_status"] == 5


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_ckeckpoint_2(client, monkeypatch, minimal_db_initialization):
    """POST /jobs/<id>/checkpoints/new"""
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    meta_schedule("internal")
    set_job_state(job_id, "Running")
    res = client.post(
        "/jobs/{job_id}/checkpoints/new".format(job_id=job_id),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    assert res.json()["exit_status"] == 0


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_signal_1(client, monkeypatch, minimal_db_initialization):
    """POST /jobs/<id>/signal/<signal>"""
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    res = client.post(
        "/jobs/{job_id}/signal/{signal}".format(job_id=job_id, signal=12),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    # Can not checkpoint job is not running
    assert res.json()["exit_status"] == 5


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_signal_2(
    client, setup_config, monkeypatch, minimal_db_initialization
):
    """POST /jobs/<id>/signal/<signal>"""
    config, _, engine = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    meta_schedule(minimal_db_initialization, config, "internal")
    set_job_state(minimal_db_initialization, job_id, "Running")
    res = client.post(
        "/jobs/{job_id}/signal/{signal}".format(job_id=job_id, signal=12),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    assert res.json()["exit_status"] == 0


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_hold_1(client, monkeypatch, minimal_db_initialization):
    """POST /jobs/<id>/holds/new"""
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    res = client.post(
        "/jobs/{job_id}/holds/new".format(job_id=job_id),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    assert res.json()["exit_status"] == 0


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_hold_2(client, monkeypatch, minimal_db_initialization):
    """POST /jobs/<id>/holds/new"""
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    meta_schedule("internal")
    set_job_state(job_id, "Running")
    res = client.post(
        "/jobs/{job_id}/holds/new".format(job_id=job_id),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    assert res.json()["exit_status"] == 1


@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_rhold_user_not_allowed_1(
    client, monkeypatch, minimal_db_initialization
):
    """POST /jobs/<id>/rhold/new"""
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    res = client.post(
        "/jobs/{job_id}/rhold/new".format(job_id=job_id),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    assert res.json()["exit_status"] == 1


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_rhold_2(client, monkeypatch, minimal_db_initialization):
    """POST /jobs/<id>/rhold/new"""
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    res = client.post(
        "/jobs/{job_id}/rhold/new".format(job_id=job_id),
        headers={"x-remote-ident": "oar"},
    )
    print(res.json())
    assert res.status_code == 200
    assert res.json()["exit_status"] == 0


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_resume_bad_nohold(client, monkeypatch, minimal_db_initialization):
    """POST /jobs/<id>/resumptions/new"""
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    res = client.post(
        "/jobs/{job_id}/resumptions/new".format(job_id=job_id),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    assert res.json()["exit_status"] == 1


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_resume_not_allowed(client, monkeypatch, minimal_db_initialization):
    """POST /jobs/<id>/resumptions/new"""
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    set_job_state(job_id, "Suspended")
    res = client.post(
        "/jobs/{job_id}/resumptions/new".format(job_id=job_id),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    assert res.json()["exit_status"] == 1


@pytest.mark.usefixtures("minimal_db_initialization")
@pytest.mark.usefixtures("monkeypatch_tools")
def test_app_jobs_resume(client, monkeypatch, minimal_db_initialization):
    """POST /jobs/<id>/resumptions/new"""
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
    )
    set_job_state(job_id, "Suspended")
    res = client.post(
        "/jobs/{job_id}/resumptions/new".format(job_id=job_id),
        headers={"x-remote-ident": "oar"},
    )
    print(res.json())
    assert res.status_code == 200
    assert res.json()["exit_status"] == 0


# @pytest.mark.usefixtures("monkeypatch_tools")
# def test_app_jobs_get_one_resources(monkeypatch):
#    """GET /jobs/<id>/resources"""
#    job_id = insert_job(minimal_db_initialization,res=[(60, [('resource_id=4', "")])], properties="")
#    meta_schedule('internal')
#    res = client.get(url_for('jobs.resources', job_id=job_id))
#    print(res.json, len(res.json()['items']))
#    assert res.json()['items'][0]['id'] == job_id
#    assert res.json()['items'][0]['type'] == 'PASSIVE'

# TODO
# @app.args({'offset': Arg(int, default=0),
#           'limit': Arg(int),
#           'user': Arg(str),
#           'from': Arg(int, dest='start_time'),
#            'to': Arg(int, dest='stop_time'),
#           'state': Arg([str, ','], dest='states'),
#           'array': Arg(int, dest='array_id'),
#           'ids'


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_job_post_bug1(client, minimal_db_initialization):
    # BUG oarapi -d {"resource":"nodes=1,walltime=00:10:0", "command":"sleep 600"}
    data = {"resource": ["nodes=1,walltime=00:10:0"], "command": 'sleep "1"'}
    res = client.post("/jobs/", json=data, headers={"x-remote-ident": "bob"})
    print(res.json())
    assert res.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_job_post_bug2(client, minimal_db_initialization):
    # BUG oarapi -d {"resource":"nodes=1,walltime=00:10:0", "command":"sleep 600"}
    data = {"resource": ["nodes=1,walltime=00:10:0"], "command": 'sleep "1"'}
    res = client.post("/jobs/", json=data, headers={"x-remote-ident": "bob"})
    print(res.json())
    assert res.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_job_post_bug3(client, minimal_db_initialization):
    # BUG oarapi -d {"resource":"nodes=1,walltime=00:10:0", "command":"sleep 600"}
    data = {
        "resource": ["nodes=1,walltime=00:10:0", "nodes=2,walltime=00:5:0"],
        "command": 'sleep "1"',
    }
    res = client.post("/jobs/", json=data, headers={"x-remote-ident": "bob"})
    print(res.json())
    assert res.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_job_post_json(client, minimal_db_initialization):
    # BUG oarapi -d {"resource":"nodes=1,walltime=00:10:0", "command":"sleep 600"}
    data = {
        "resource": ["nodes=1,walltime=00:10:0", "nodes=2,walltime=00:5:0"],
        "command": 'sleep "prout"',
    }
    res = client.post(
        "/jobs/",
        json=data,
        # content_type="application/json",
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200


@pytest.mark.usefixtures("minimal_db_initialization")
def test_app_job_post_array(client, minimal_db_initialization):
    data = {
        "resource": ["nodes=1,walltime=00:10:0"],
        "command": 'sleep "1"',
        "param_file": "param9 9\nparam8 8\nparam7 7",
    }
    res = client.post("/jobs/", json=data, headers={"x-remote-ident": "bob"})
    print(res.__dict__)
    print(res.json())
    job_array_ids = (
        db.query(Job.id, Job.array_id, Job.array_index, Job.command)
        .order_by(Job.id)
        .all()
    )
    print(job_array_ids)
    assert res.status_code == 200
