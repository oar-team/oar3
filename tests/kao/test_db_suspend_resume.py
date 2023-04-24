# coding: utf-8
import pytest

import oar.lib.tools  # for monkeypatching
from oar.kao.meta_sched import meta_schedule
from oar.lib.job_handling import insert_job, set_job_state


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db["Queue"].create(
            name="default", priority=3, scheduler_policy="kamelot", state="Active"
        )

        # add some resources
        for i in range(5):
            db["Resource"].create(network_address="localhost" + str(int(i / 2)))
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


@pytest.fixture(scope="function")
def config_suspend_resume(request):
    config["JUST_BEFORE_RESUME_EXEC_FILE"] = "true"
    config["SUSPEND_RESUME_SCRIPT_TIMEOUT"] = "1"

    def teardown():
        del config["JUST_BEFORE_RESUME_EXEC_FILE"]
        del config["SUSPEND_RESUME_SCRIPT_TIMEOUT"]

    request.addfinalizer(teardown)


@pytest.mark.usefixtures("config_suspend_resume")
def test_suspend_resume_1(monkeypatch):
    # now = get_date()
    insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    meta_schedule("internal")
    job = db["Job"].query.one()
    print(job.state)
    set_job_state(job.id, "Resuming")
    job = db["Job"].query.one()
    print(job.state)
    meta_schedule("internal")
    assert job.state == "Resuming"
    # assert(True)


@pytest.mark.usefixtures("config_suspend_resume")
def test_suspend_resume_2(monkeypatch):
    config["JUST_BEFORE_RESUME_EXEC_FILE"] = "sleep 2"
    # now = get_date()
    insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    meta_schedule("internal")
    job = db["Job"].query.one()
    print(job.state)
    set_job_state(job.id, "Resuming")
    job = db["Job"].query.one()
    print(job.state)
    meta_schedule("internal")
    assert job.state == "Resuming"
