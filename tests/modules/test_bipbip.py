# coding: utf-8

import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import get_job_cpuset_name, insert_job
from oar.lib.models import (
    AssignedResource,
    Challenge,
    EventLog,
    Job,
    JobType,
    MoldableJobDescription,
    Resource,
)
from oar.modules.bipbip import BipBip

from ..faketools import FakePopen, fake_popen

# _, db, logger = init_oar()
fake_bad_nodes = {"pingchecker": (1, []), "init": [], "clean": []}
fake_tag = 1


def set_fake_tag(tag_value):
    global fake_tag
    fake_tag = tag_value


def fake_pingchecker(hosts):
    return fake_bad_nodes["pingchecker"]


def fake_launch_oarexec(cmt, data, oarexec_files):
    return True


# calls to the job resource manager: (action, data sent to the JRM)
remote_calls = []


def fake_manage_remote_commands(
    hosts, data_str, manage_file, action, ssh_command, taktuk_cmd=None
):
    remote_calls.append((action, data_str))
    return (fake_tag, fake_bad_nodes[action])


@pytest.fixture(scope="function", autouse=True)
def builtin_config(request, setup_config):
    config, _ = setup_config

    config.setdefault_config(
        {"CPUSET_PATH": "/oar", "JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD": "cpuset"}
    )

    yield config


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources
        for i in range(5):
            Resource.create(session, network_address="localhost" + str(i))
        yield session


def fake_notify_interactif_user(job, y):
    job.info_type = "test:0000"
    return None


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    remote_calls.clear()
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda x, y: None)
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


def test_bipbip_void(setup_config, minimal_db_initialization):
    config, engine = setup_config
    bipbip = BipBip([], config)
    bipbip.run(minimal_db_initialization, config)
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1


def test_bipbip_simple(setup_config, minimal_db_initialization):
    config, engine = setup_config
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    Challenge.create(
        minimal_db_initialization,
        job_id=job_id,
        challenge="foo1",
        ssh_private_key="foo2",
        ssh_public_key="foo2",
    )

    # Bipbip needs a job id
    bipbip = BipBip([job_id], config)
    bipbip.run(minimal_db_initialization, config)
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1


def _test_bipbip_toLaunch(
    session, config, types=[], job_id=None, state="toLaunch", args=[]
):
    if not job_id:
        if "envelope" in types:
            nb_resources = 0
        else:
            nb_resources = 4
        (job_id, moldable_id) = insert_job(
            session,
            res=[(60, [(f"resource_id={nb_resources}", "")])],
            properties="",
            command="yop",
            state=state,
            user="l_impertinent_survolte",
            stdout_file="poy",
            stderr_file="yop",
            types=types,
            return_moldable=True,
        )
    else:
        moldable_id = [
            row[0]
            for row in session.query(MoldableJobDescription.id)
            .filter(MoldableJobDescription.job_id == job_id)
            .all()
        ]

    session.query(Job).update(
        {Job.assigned_moldable_job: moldable_id[0]}, synchronize_session=False
    )
    Challenge.create(
        session,
        job_id=job_id,
        challenge="foo1",
        ssh_private_key="foo2",
        ssh_public_key="foo2",
    )

    # db.commit()
    # import pdb; pdb.set_trace()
    resources = session.query(Resource).all()

    if "envelope" in types:
        AssignedResource.create(session, moldable_id=moldable_id[0], resource_id=0)
    else:
        for r in resources[:4]:
            AssignedResource.create(
                session, moldable_id=moldable_id[0], resource_id=r.id
            )
            print(r.id, r.network_address)
    session.commit()

    for ass_res in session.query(AssignedResource).all():
        print("AssignedResource:", ass_res.moldable_id, ass_res.resource_id)

    config["SERVER_HOSTNAME"] = "localhost"
    config["DETACH_JOB_FROM_SERVER"] = "localhost"

    # Bipbip needs a job id
    bipbip = BipBip([job_id] + args, config=config)
    bipbip.run(session, config)

    return job_id, bipbip


def test_bipbip_toLaunch(minimal_db_initialization, builtin_config):
    _, bipbip = _test_bipbip_toLaunch(minimal_db_initialization, builtin_config)
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_noop(minimal_db_initialization, builtin_config):
    _, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization, builtin_config, types=["noop"]
    )
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_cpuset_error(minimal_db_initialization, builtin_config):
    # import pdb; pdb.set_trace()
    fake_bad_nodes["init"] = ["localhost0"]
    job_id, bipbip = _test_bipbip_toLaunch(minimal_db_initialization, builtin_config)
    fake_bad_nodes["init"] = []
    event = (
        minimal_db_initialization.query(EventLog)
        .filter(EventLog.job_id == job_id)
        .first()
    )

    print(bipbip.exit_code)
    assert event.type == "CPUSET_ERROR"
    assert bipbip.exit_code == 2


def test_bipbip_toLaunch_cpuset_error_advance_reservation(
    monkeypatch, minimal_db_initialization, builtin_config
):
    (job_id, moldable_id) = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        command="yop",
        state="toLaunch",
        stdout_file="poy",
        stderr_file="yop",
        reservation="Scheduled",
        return_moldable=True,
    )
    fake_bad_nodes["init"] = ["localhost0"]
    _, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization, builtin_config, job_id=job_id
    )
    fake_bad_nodes["init"] = []
    event = (
        minimal_db_initialization.query(EventLog)
        .filter(EventLog.job_id == job_id)
        .first()
    )

    print(bipbip.exit_code)
    assert event.type == "CPUSET_ERROR"
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_server_prologue(minimal_db_initialization, builtin_config):
    builtin_config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch(minimal_db_initialization, builtin_config)
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_server_prologue_env(minimal_db_initialization, builtin_config):
    builtin_config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization, builtin_config, types=["test=lol", "yop"]
    )
    print(bipbip.exit_code)

    # Doing this because depending on the db types the order is different
    assert set(["yop=1", "test=lol"]) == set(
        fake_popen["env"]["OAR_JOB_TYPES"].split(";")
    )

    fake_popen["env"] = {}
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_server_prologue_return_code(
    minimal_db_initialization, builtin_config
):
    fake_popen["wait_return_code"] = 1
    builtin_config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch(minimal_db_initialization, builtin_config)
    fake_popen["wait_return_code"] = 0
    print(bipbip.exit_code)
    assert bipbip.exit_code == 2


def test_bipbip_toLaunch_server_prologue_return_code_interactive(
    minimal_db_initialization, builtin_config
):
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        command="yop",
        state="toLaunch",
        stdout_file="poy",
        stderr_file="yop",
        job_type="INTERACTIVE",
    )
    fake_popen["wait_return_code"] = 1
    builtin_config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization, builtin_config, job_id=job_id
    )
    fake_popen["wait_return_code"] = 0
    print(bipbip.exit_code)
    assert bipbip.exit_code == 2


def test_bipbip_toLaunch_server_prologue_OSError(
    minimal_db_initialization, builtin_config
):
    fake_popen["exception"] = "OSError"
    builtin_config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch(minimal_db_initialization, builtin_config)
    fake_popen["exception"] = None
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_server_prologue_TimeoutExpired(
    minimal_db_initialization, builtin_config
):
    fake_popen["exception"] = "TimeoutExpired"
    builtin_config["SERVER_PROLOGUE_EXEC_FILE"] = "foo_script"
    _, bipbip = _test_bipbip_toLaunch(minimal_db_initialization, builtin_config)
    fake_popen["exception"] = None
    assert bipbip.exit_code == 2


def test_bipbip_running_oarexec_reattachexit_value(
    minimal_db_initialization, builtin_config
):
    job_id, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization,
        builtin_config,
        state="Running",
        args=["1", "2", "foo1"],
    )
    assert bipbip.exit_code == 0


def test_bipbip_running_oarexec_reattachexit_value_bad_challenge(
    minimal_db_initialization,
    builtin_config,
):
    job_id, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization,
        builtin_config,
        state="Running",
        args=["1", "2", "bad_challenge"],
    )
    event = (
        minimal_db_initialization.query(EventLog)
        .filter(EventLog.job_id == job_id)
        .first()
    )
    assert event.type == "BIPBIP_CHALLENGE"
    assert bipbip.exit_code == 2


def test_bipbip_running_oarexec_reattachexit_bad_value(
    setup_config, minimal_db_initialization, builtin_config
):
    job_id, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization,
        builtin_config,
        state="Running",
        args=["bug", "2", "foo1"],
    )
    assert bipbip.exit_code == 2


def test_bipbip_toLaunch_envelope(minimal_db_initialization, builtin_config):
    _, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization, builtin_config, types=["envelope"]
    )
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_supersed(minimal_db_initialization, builtin_config):
    _, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization, builtin_config, types=["supersed=1"]
    )
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0


def test_bipbip_toLaunch_leaflet(minimal_db_initialization, builtin_config):
    job_id, bipbip = _test_bipbip_toLaunch(
        minimal_db_initialization, builtin_config, types=["leaflet=12345"]
    )
    print(bipbip.exit_code)
    assert bipbip.exit_code == 0
    assert (
        get_job_cpuset_name(minimal_db_initialization, job_id)
        == "l_impertinent_survolte_12345"
    )


def _insert_assigned_job(session, user, state, types=[], nb_resources=4):
    """Insert a job with its own assigned moldable and the first nb_resources resources"""
    (job_id, moldable_id) = insert_job(
        session,
        res=[(60, [(f"resource_id={nb_resources}", "")])],
        properties="",
        command="yop",
        state=state,
        user=user,
        stdout_file="poy",
        stderr_file="yop",
        types=types,
        return_moldable=True,
    )
    session.query(Job).filter(Job.id == job_id).update(
        {Job.assigned_moldable_job: moldable_id[0], Job.start_time: 10},
        synchronize_session=False,
    )
    Challenge.create(
        session,
        job_id=job_id,
        challenge="foo1",
        ssh_private_key="foo2",
        ssh_public_key="foo2",
    )
    for r in session.query(Resource).order_by(Resource.id).all()[:nb_resources]:
        AssignedResource.create(session, moldable_id=moldable_id[0], resource_id=r.id)
    session.commit()
    return job_id


def _job_events(session, job_id):
    return [e.type for e in session.query(EventLog).filter(EventLog.job_id == job_id)]


def _run_supersed(session, config, old_user, old_state, new_user="toto"):
    """Job N (old) and job N+1 (supersed=N) on the same resources; run bipbip on N+1"""
    old_id = _insert_assigned_job(session, old_user, old_state)
    new_id = _insert_assigned_job(
        session, new_user, "toLaunch", types=[f"supersed={old_id}"]
    )
    config["SERVER_HOSTNAME"] = "localhost"
    config["DETACH_JOB_FROM_SERVER"] = "localhost"
    bipbip = BipBip([new_id], config=config)
    bipbip.run(session, config)
    session.expire_all()
    old = session.query(Job).filter(Job.id == old_id).one()
    new = session.query(Job).filter(Job.id == new_id).one()
    return old, new, bipbip


def test_bipbip_supersed_running_job(minimal_db_initialization, builtin_config):
    session = minimal_db_initialization
    old, new, bipbip = _run_supersed(session, builtin_config, "toto", "Running")

    assert bipbip.exit_code == 0
    assert new.state == "Running"

    # the id of the superseded job is sent to the job resource manager (init of N+1)
    init_data = [d for (action, d) in remote_calls if action == "init"]
    assert len(init_data) == 1
    assert f"'supersed_job_id' => {old.id}," in init_data[0]

    # the superseded job is finished: Finishing, its cgroups are cleaned and
    # node_change_state will switch it to Terminated
    assert old.state == "Finishing"
    clean_data = [d for (action, d) in remote_calls if action == "clean"]
    assert len(clean_data) == 1
    assert f"'job_id' => {old.id}," in clean_data[0]
    events = _job_events(session, old.id)
    assert "SUPERSEDED" in events
    assert "SWITCH_INTO_TERMINATE_STATE" in events


def test_bipbip_supersed_job_not_running(minimal_db_initialization, builtin_config):
    session = minimal_db_initialization
    old, new, bipbip = _run_supersed(session, builtin_config, "toto", "Terminated")

    assert bipbip.exit_code == 0
    assert new.state == "Running"
    assert old.state == "Terminated"
    assert "SUPERSEDED" not in _job_events(session, old.id)
    assert [action for (action, _) in remote_calls] == ["init"]


def test_bipbip_supersed_job_of_another_user(minimal_db_initialization, builtin_config):
    session = minimal_db_initialization
    old, new, bipbip = _run_supersed(session, builtin_config, "titi", "Running")

    assert bipbip.exit_code == 0
    assert new.state == "Running"
    assert old.state == "Running"
    assert "SUPERSEDED" not in _job_events(session, old.id)


def test_bipbip_supersed_bad_value(minimal_db_initialization, builtin_config):
    session = minimal_db_initialization
    job_id = _insert_assigned_job(session, "toto", "toLaunch", types=["supersed=abc"])
    bipbip = BipBip([job_id], config=builtin_config)
    bipbip.run(session, builtin_config)

    assert bipbip.exit_code == 0
    init_data = [d for (action, d) in remote_calls if action == "init"]
    assert "'supersed_job_id' => undef," in init_data[0]


def test_bipbip_supersed_itself(minimal_db_initialization, builtin_config):
    session = minimal_db_initialization
    job_id = _insert_assigned_job(session, "toto", "toLaunch")
    session.query(JobType).filter(JobType.job_id == job_id).delete()
    JobType.create(session, job_id=job_id, type=f"supersed={job_id}")
    session.commit()
    bipbip = BipBip([job_id], config=builtin_config)
    bipbip.run(session, builtin_config)
    session.expire_all()

    job = session.query(Job).filter(Job.id == job_id).one()
    assert job.state == "Running"
    assert "SUPERSEDED" not in _job_events(session, job_id)
