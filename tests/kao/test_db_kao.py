# coding: utf-8
import pytest

import oar.lib.tools  # for monkeypatching
from oar.kao.kao import main
from oar.lib import config, db
from oar.lib.job_handling import insert_job


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config["METASCHEDULER_MODE"] = "internal"

    @request.addfinalizer
    def teardown():
        config["METASCHEDULER_MODE"]


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db["Queue"].create(
            name="default", priority=3, scheduler_policy="kamelot", state="Active"
        )

        # add some resources
        for i in range(5):
            db["Resource"].create(network_address="localhost")
        yield


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "notify_tcp_socket", lambda addr, port, msg: len(msg)
    )
    monkeypatch.setattr(
        oar.lib.tools, "notify_user", lambda job, state, msg: len(state + msg)
    )
    monkeypatch.setattr(oar.lib.tools, "notify_bipbip_commander", lambda json_msg: True)


def test_db_kao_simple_1(monkeypatch):
    insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    job = db["Job"].query.one()
    print("job state:", job.state)

    main()

    for i in db["GanttJobsPrediction"].query.all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time)

    job = db["Job"].query.one()
    print(job.state)

    assert job.state == "toLaunch"


def test_db_kao_moldable(monkeypatch):
    # First moldable job should never pass because there is not enough resources.
    insert_job(
        res=[(5, [("resource_id=6", "")]), (2, [("resource_id=2", "")])], properties=""
    )
    job = db["Job"].query.one()

    print("job state:", job.state)

    main()

    for i in db["GanttJobsPrediction"].query.all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time)

    job = db["Job"].query.one()
    print(job.state)

    assert job.state == "toLaunch"
