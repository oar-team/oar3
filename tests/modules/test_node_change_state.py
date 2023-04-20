# coding: utf-8
import os

import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import (
    AssignedResource,
    Challenge,
    EventLog,
    FragJob,
    Job,
    Resource,
    MoldableJobDescription,
)
from oar.modules.node_change_state import NodeChangeState, main

fake_manage_remote_commands_return = (1, [])


def fake_manage_remote_commands(
    hosts, data_str, manage_file, action, ssh_command, taktuk_cmd=None
):
    return fake_manage_remote_commands_return


fake_exec_with_timeout_return = ""


def fake_exec_with_timeout(args, timeout):
    return fake_exec_with_timeout_return


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources
        for i in range(5):
            Resource.create(session, network_address="localhost" + str(i))
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "manage_remote_commands", fake_manage_remote_commands
    )
    monkeypatch.setattr(oar.lib.tools, "exec_with_timeout", fake_exec_with_timeout)
    monkeypatch.setattr(oar.lib.tools, "notify_tcp_socket", lambda x, y, z: None)


def assign_resources(session, job_id):
    session.query(Job).filter(Job.id == job_id).update(
        {Job.assigned_moldable_job: job_id}, synchronize_session=False
    )
    resources = session.query(Resource).all()
    for r in resources[:4]:
        AssignedResource.create(session, moldable_id=job_id, resource_id=r.id)


def test_node_change_state_main():
    exit_code = main()
    print(exit_code)
    assert exit_code == 0


def test_node_change_state_void(minimal_db_initialization):
    node_change_state = NodeChangeState()
    node_change_state.run()
    print(node_change_state.exit_code)
    assert node_change_state.exit_code == 0


def base_test_node_change(
    session, config, event_type, job_state, job_id=None, exit_code=0
):
    if not job_id:
        job_id = insert_job(session, res=[(60, [("resource_id=4", "")])], properties="")

    EventLog.create(session, to_check="YES", job_id=job_id, type=event_type)
    os.environ["OARDO_USER"] = "oar"

    node_change_state = NodeChangeState(config)
    node_change_state.run(session)

    job = session.query(Job).filter(Job.id == job_id).first()
    print(node_change_state.exit_code)
    assert node_change_state.exit_code == exit_code
    assert job.state == job_state


def test_node_change_state_error(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    base_test_node_change(minimal_db_initialization, config, "EXTERMINATE_JOB", "Error")


def test_node_change_state_job_idempotent_exitcode_25344(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        exit_code=25344,
        types=["idempotent", "timesharing=*,*"],
        start_time=10,
        stop_time=100,
    )
    base_test_node_change(
        minimal_db_initialization,
        config,
        "SWITCH_INTO_TERMINATE_STATE",
        "Terminated",
        job_id,
    )


def test_node_change_state_job_switch_to_error(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    base_test_node_change(
        minimal_db_initialization, config, "SWITCH_INTO_ERROR_STATE", "Error"
    )


def test_node_change_state_job_epilogue_error(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    base_test_node_change(
        minimal_db_initialization, config, "EPILOGUE_ERROR", "Terminated"
    )


def test_node_change_state_job_FRAG_JOB_REQUEST(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    base_test_node_change(
        minimal_db_initialization, config, "FRAG_JOB_REQUEST", "Waiting"
    )


def test_node_change_state_PING_CHECKER_NODE_SUSPECTED(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
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
    base_test_node_change(
        minimal_db_initialization,
        config,
        "PING_CHECKER_NODE_SUSPECTED",
        "Error",
        job_id,
    )


def test_node_change_state_job_scheduled_prologue_error(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        start_time=10,
        stop_time=100,
        reservation="Scheduled",
    )
    # import pdb; pdb.set_trace()
    assign_resources(minimal_db_initialization, job_id)
    base_test_node_change(
        minimal_db_initialization, config, "PROLOGUE_ERROR", "Error", job_id, 1
    )


def test_node_change_state_job_check_toresubmit(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
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
    base_test_node_change(
        minimal_db_initialization, config, "SERVER_PROLOGUE_TIMEOUT", "Error", job_id
    )
    event = (
        minimal_db_initialization.query(EventLog)
        .filter(EventLog.type == "RESUBMIT_JOB_AUTOMATICALLY")
        .first()
    )
    assert event.job_id == job_id


def test_node_change_state_job_suspend_resume(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
    )

    setup_config["JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD"] = "core"
    setup_config["SUSPEND_RESUME_FILE"] = "/tmp/fake_suspend_resume"
    setup_config["JUST_AFTER_SUSPEND_EXEC_FILE"] = "/tmp/fake_admin_script"
    setup_config["SUSPEND_RESUME_SCRIPT_TIMEOUT"] = 60

    assign_resources(minimal_db_initialization, job_id)
    base_test_node_change(
        minimal_db_initialization,
        config,
        "HOLD_WAITING_JOB",
        "Suspended",
        job_id,
        config=setup_config,
    )


def test_node_change_state_job_suspend_resume_error(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    global fake_exec_with_timeout_return
    fake_exec_with_timeout_return = "foo_msg_error"

    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
    )

    config["JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD"] = "core"
    config["SUSPEND_RESUME_FILE"] = "/tmp/fake_suspend_resume"
    config["JUST_AFTER_SUSPEND_EXEC_FILE"] = "/tmp/fake_admin_script"
    config["SUSPEND_RESUME_SCRIPT_TIMEOUT"] = 60

    assign_resources(minimal_db_initialization, job_id)
    base_test_node_change(
        minimal_db_initialization, config, "HOLD_WAITING_JOB", "Resuming", job_id
    )

    event = (
        minimal_db_initialization.query(EventLog)
        .filter(EventLog.type == "SUSPEND_SCRIPT_ERROR")
        .one()
    )
    assert event.job_id == job_id


def test_node_change_state_job_suspend_resume_suspend_tag0(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    global fake_manage_remote_commands_return
    fake_manage_remote_commands_return = (0, [])

    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
    )

    config["JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD"] = "core"
    config["SUSPEND_RESUME_FILE"] = "/tmp/fake_suspend_resume"
    config["JUST_AFTER_SUSPEND_EXEC_FILE"] = "/tmp/fake_admin_script"
    config["SUSPEND_RESUME_SCRIPT_TIMEOUT"] = 60

    assign_resources(minimal_db_initialization, job_id)
    base_test_node_change(
        minimal_db_initialization, config, "HOLD_WAITING_JOB", "Running", job_id
    )

    event = (
        minimal_db_initialization.query(EventLog)
        .filter(EventLog.type == "SUSPEND_RESUME_MANAGER_FILE")
        .one()
    )
    assert event.job_id == job_id


def test_node_change_state_job_suspend_resume_waiting_interactive(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Waiting",
        job_type="INTERACTIVE",
        info_type="123.123.123.123:1234",
    )
    base_test_node_change(
        minimal_db_initialization, config, "HOLD_WAITING_JOB", "Hold", job_id
    )


def test_node_change_state_job_suspend_resume_resuming(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", state="Resuming"
    )
    base_test_node_change(
        minimal_db_initialization, config, "HOLD_WAITING_JOB", "Suspended", job_id
    )


def test_node_change_state_job_suspend_resume_suspend(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", state="Suspend"
    )
    base_test_node_change(
        minimal_db_initialization, config, "RESUME_JOB", "Resuming", job_id
    )


def test_node_change_state_job_suspend_resume_hold(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])], properties="", state="Hold"
    )
    base_test_node_change(
        minimal_db_initialization, config, "RESUME_JOB", "Waiting", job_id
    )


def test_node_change_state_resource_suspected(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    minimal_db_initialization.query(Resource).filter(
        Resource.network_address == "localhost0"
    ).update({Resource.next_state: "Suspected"}, synchronize_session=False)
    base_test_node_change(
        minimal_db_initialization,
        config,
        "SWITCH_INTO_TERMINATE_STATE",
        "Terminated",
        None,
        1,
    )


def test_node_change_state_resource_dead(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    minimal_db_initialization.query(Resource).filter(
        Resource.network_address == "localhost0"
    ).update({Resource.next_state: "Dead"}, synchronize_session=False)
    base_test_node_change(
        minimal_db_initialization,
        config,
        "SWITCH_INTO_TERMINATE_STATE",
        "Terminated",
        None,
        1,
    )


def test_node_change_state_resource_dead_assigned(
    minimal_db_initialization, setup_config
):
    (
        config,
        _,
        _,
    ) = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
    )
    assign_resources(minimal_db_initialization, job_id)
    minimal_db_initialization.query(Resource).filter(
        Resource.network_address == "localhost0"
    ).update({Resource.next_state: "Dead"}, synchronize_session=False)
    node_change_state = NodeChangeState(config)
    node_change_state.run(minimal_db_initialization)
    assert node_change_state.exit_code == 2


def assign_resources_with_range(session, job_id, from_, to_):

    moldable = (
        session.query(MoldableJobDescription)
        .filter(MoldableJobDescription.job_id == job_id)
        .first()
    )

    session.query(Job).filter(Job.id == job_id).update(
        {Job.assigned_moldable_job: moldable.id}, synchronize_session=False
    )
    resources = session.query(Resource).all()

    for r in resources[from_:to_]:
        AssignedResource.create(session, moldable_id=moldable.id, resource_id=r.id)


def test_node_change_state_job_cosystem(minimal_db_initialization, setup_config):
    """
    Check that noop, and cosystem jobs are not killed. But other jobs are.
    """
    setup_config, _, _ = setup_config
    cosystem_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        properties="",
        state="Running",
        types=["cosystem"],
        info_type="123.123.123.123:1234",
    )
    assign_resources_with_range(minimal_db_initialization, cosystem_id, 0, 2)

    noop_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        properties="",
        state="Running",
        types=["noop"],
        info_type="123.123.123.123:1234",
    )
    assign_resources_with_range(minimal_db_initialization, noop_id, 2, 4)

    # Third job that should be killed
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=1", "")])],
        properties="",
        state="Running",
        types=[],
        info_type="123.123.123.123:1234",
    )
    assign_resources_with_range(minimal_db_initialization, job_id, 4, 5)

    resources = minimal_db_initialization.query(Resource).all()
    for r in resources:
        minimal_db_initialization.query(Resource).filter(
            Resource.network_address == r.network_address
        ).update({Resource.next_state: "Dead"}, synchronize_session=False)

    os.environ["OARDO_USER"] = "oar"

    node_change_state = NodeChangeState(setup_config)
    node_change_state.run(minimal_db_initialization)

    fragjob = minimal_db_initialization.query(FragJob).first()
    assert fragjob is not None and fragjob.job_id == job_id
