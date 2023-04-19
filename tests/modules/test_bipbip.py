# coding: utf-8
import pytest

import oar.lib.tools  # for monkeypatching
from oar.lib.models import AssignedResource, Challenge, EventLog, Job, Resource
from oar.lib.job_handling import insert_job
from oar.lib.globals import init_oar
from oar.modules.bipbip import BipBip

from ..faketools import FakePopen, fake_popen

config, db, logger = init_oar()
fake_bad_nodes = {"pingchecker": (1, []), "init": [], "clean": []}
fake_tag = 1


def set_fake_tag(tag_value):
    global fake_tag
    fake_tag = tag_value


def fake_pingchecker(hosts):
    return fake_bad_nodes["pingchecker"]


def fake_launch_oarexec(cmt, data, oarexec_files):
    return True


def fake_manage_remote_commands(
    hosts, data_str, manage_file, action, ssh_command, taktuk_cmd=None
):
    return (fake_tag, fake_bad_nodes[action])


@pytest.fixture(scope="function", autouse=True)
def builtin_config(request):
    config.setdefault_config(
        {"CPUSET_PATH": "/oar", "JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD": "cpuset"}
    )


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            Resource.create(network_address="localhost" + str(i))
        yield


def fake_notify_interactif_user(job, y):
    job.info_type = "test:0000"
    return None


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(oar.lib.tools, "pingchecker", fake_pingchecker)
    monkeypatch.setattr(
        oar.lib.tools, "notify_interactif_user", fake_notify_interactif_user
    )
    monkeypatch.setattr(oar.lib.tools, "launch_oarexec", fake_launch_oarexec)
    monkeypatch.setattr(
        oar.lib.tools, "manage_remote_commands", fake_manage_remote_commands
    )
    monkeypatch.setattr(oar.lib.tools, "Popen", FakePopen)
    monkeypatch.setattr(oar.lib.tools, "kill_child_processes", lambda x: None)


def test_bipbip_void():
    bipbip = BipBip(None)
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1


def test_bipbip_simple():
    job_id = insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    Challenge.create(
        job_id=job_id, challenge="foo1", ssh_private_key="foo2", ssh_public_key="foo2"
    )

    # Bipbip needs a job id
    bipbip = BipBip([job_id])
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1


def _test_bipbip_toLaunch(types=[], job_id=None, state="toLaunch", args=[]):
    if not job_id:
        job_id = insert_job(
            res=[(60, [("resource_id=4", "")])],
            properties="",
            command="yop",
            state=state,
            stdout_file="poy",
            stderr_file="yop",
            types=types,
        )
    db.query(Job).update({Job.assigned_moldable_job: job_id}, synchronize_session=False)
    Challenge.create(
        job_id=job_id, challenge="foo1", ssh_private_key="foo2", ssh_public_key="foo2"
    )

    # db.commit()
    # import pdb; pdb.set_trace()
    resources = db.query(Resource).all()

    print("yop")
    print(resources)

    for r in resources[:4]:
        AssignedResource.create(moldable_id=job_id, resource_id=r.id)
        print(r.id, r.network_address)
    db.commit()

    for ass_res in db.query(AssignedResource).all():
        print("AssignedResource:", ass_res.moldable_id, ass_res.resource_id)

    config["SERVER_HOSTNAME"] = "localhost"
    config["DETACH_JOB_FROM_SERVER"] = "localhost"

    # Bipbip needs a job id
    bipbip = BipBip([job_id] + args)
    bipbip.run()

    return job_id, bipbip


def test_bipbip_toLaunch():
    _, bipbip = _test_bipbip_toLaunch()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_noop():
    _, bipbip = _test_bipbip_toLaunch(types=["noop"])
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_cpuset_error():
    # import pdb; pdb.set_trace()
    fake_bad_nodes["init"] = ["localhost0"]
    job_id, bipbip = _test_bipbip_toLaunch()
    fake_bad_nodes["init"] = []
    event = db.query(EventLog).filter(EventLog.job_id == job_id).first()

    print(bipbip.exit_code)
    assert event.type == "CPUSET_ERROR"
    assert bipbip.exit_code == 2


def test_bipbip_toLaunch_cpuset_error_advance_reservation(monkeypatch):
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])],
        properties="",
        command="yop",
        state="toLaunch",
        stdout_file="poy",
        stderr_file="yop",
        reservation="Scheduled",
    )
    fake_bad_nodes["init"] = ["localhost0"]
    _, bipbip = _test_bipbip_toLaunch(job_id=job_id)
    fake_bad_nodes["init"] = []
    event = db.query(EventLog).filter(EventLog.job_id == job_id).first()

    print(bipbip.exit_code)
    assert event.type == "CPUSET_ERROR"
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_server_prologue():
    config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_server_prologue_env():
    config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch(types=["test=lol", "yop"])
    print(bipbip.exit_code)

    # Doing this because depending on the db types the order is different
    assert set(["yop=1", "test=lol"]) == set(
        fake_popen["env"]["OAR_JOB_TYPES"].split(";")
    )

    fake_popen["env"] = {}
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_server_prologue_return_code():
    fake_popen["wait_return_code"] = 1
    config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch()
    fake_popen["wait_return_code"] = 0
    print(bipbip.exit_code)
    assert bipbip.exit_code == 2


def test_bipbip_toLaunch_server_prologue_return_code_interactive():
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])],
        properties="",
        command="yop",
        state="toLaunch",
        stdout_file="poy",
        stderr_file="yop",
        job_type="INTERACTIVE",
    )
    fake_popen["wait_return_code"] = 1
    config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch(job_id=job_id)
    fake_popen["wait_return_code"] = 0
    print(bipbip.exit_code)
    assert bipbip.exit_code == 2


def test_bipbip_toLaunch_server_prologue_OSError():
    fake_popen["exception"] = "OSError"
    config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch()
    fake_popen["exception"] = None
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_server_prologue_TimeoutExpired():
    fake_popen["exception"] = "TimeoutExpired"
    config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch()
    fake_popen["exception"] = None
    assert bipbip.exit_code == 2


def test_bipbip_running_oarexec_reattachexit_value():
    job_id, bipbip = _test_bipbip_toLaunch(state="Running", args=["1", "2", "foo1"])
    assert bipbip.exit_code == 0


def test_bipbip_running_oarexec_reattachexit_value_bad_challenge():
    job_id, bipbip = _test_bipbip_toLaunch(
        state="Running", args=["1", "2", "bad_challenge"]
    )
    event = db.query(EventLog).filter(EventLog.job_id == job_id).first()
    assert event.type == "BIPBIP_CHALLENGE"
    assert bipbip.exit_code == 2


def test_bipbip_running_oarexec_reattachexit_bad_value():
    job_id, bipbip = _test_bipbip_toLaunch(state="Running", args=["bug", "2", "foo1"])
    assert bipbip.exit_code == 2
