# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.lib.database import ephemeral_session
from oar.lib.globals import init_oar
from oar.lib.job_handling import insert_job
from oar.lib.logging import get_logger
from oar.lib.models import AssignedResource, EventLog, FragJob, Job, Resource
from oar.modules.leon import Leon

_, _, log = init_oar()
logger = get_logger(log, "test_leon")


@pytest.fixture(scope="module", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(
        oar.lib.tools, "notify_almighty", lambda x, y=None, z=None: True
    )
    monkeypatch.setattr(oar.lib.tools, "notify_bipbip_commander", lambda x: True)
    monkeypatch.setattr(oar.lib.tools, "notify_tcp_socket", lambda x, y, z: True)
    monkeypatch.setattr(oar.lib.tools, "signal_oarexec", lambda v, w, x, y, z,: "yop")


def assign_resources(session, job_id):
    session.query(Job).filter(Job.id == job_id).update(
        {Job.assigned_moldable_job: job_id}, synchronize_session=False
    )
    resources = session.query(Resource).all()
    for r in resources[:4]:
        AssignedResource.create(session, moldable_id=job_id, resource_id=r.id)


def test_leon_void(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    # Leon needs of job id
    leon = Leon(
        config,
        logger,
    )
    leon.run(minimal_db_initialization)
    print(leon.exit_code)
    assert leon.exit_code == 0


def test_leon_simple(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )
    leon = Leon(config, logger, [str(job_id)])
    leon.run(minimal_db_initialization)
    print(leon.exit_code)
    assert leon.exit_code == 0


def test_leon_simple_not_job_id_int(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    leon = Leon(config, logger, "zorglub")
    leon.run(minimal_db_initialization)
    print(leon.exit_code)
    assert leon.exit_code == 1


def test_leon_exterminate_jobid(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )

    FragJob.create(minimal_db_initialization, job_id=job_id, state="LEON_EXTERMINATE")
    print("job_id:" + str(job_id))

    leon = Leon(config, logger, [str(job_id)])
    leon.run(minimal_db_initialization)

    event = (
        minimal_db_initialization.query(EventLog)
        .filter(EventLog.type == "EXTERMINATE_JOB")
        .filter(EventLog.job_id == job_id)
        .first()
    )

    for e in minimal_db_initialization.query(EventLog).all():
        print(EventLog.type, str(EventLog.job_id))

    print(leon.exit_code)
    assert leon.exit_code == 0
    assert event.job_id == job_id


def test_leon_exterminate(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization, res=[(60, [("resource_id=4", "")])], properties=""
    )

    FragJob.create(minimal_db_initialization, job_id=job_id, state="LEON_EXTERMINATE")
    print("job_id:" + str(job_id))

    leon = Leon(
        config,
        logger,
    )
    leon.run(minimal_db_initialization)

    job = minimal_db_initialization.query(Job).filter(Job.id == job_id).first()

    print(leon.exit_code)
    assert leon.exit_code == 0
    assert job.state == "Finishing"


@pytest.mark.skip()
def test_leon_get_jobs_to_kill_waiting(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Waiting",
        job_type="INTERACTIVE",
        info_type="123.123.123.123:1234",
    )

    FragJob.create(minimal_db_initialization, job_id=job_id, state="LEON")

    leon = Leon(
        config,
        logger,
    )
    leon.run(minimal_db_initialization)

    job = minimal_db_initialization.query(Job).filter(Job.id == job_id).first()

    print(leon.exit_code)
    assert leon.exit_code == 1
    assert job.state == "Error"


def test_leon_get_jobs_to_kill_terminated(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Terminated",
    )

    FragJob.create(minimal_db_initialization, job_id=job_id, state="LEON")

    leon = Leon(
        config,
        logger,
    )
    leon.run(minimal_db_initialization)

    job = minimal_db_initialization.query(Job).filter(Job.id == job_id).first()

    print(leon.exit_code)
    assert leon.exit_code == 0
    assert job.state == "Terminated"


def test_leon_get_jobs_to_kill_noop(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
        types=["noop"],
    )

    FragJob.create(minimal_db_initialization, job_id=job_id, state="LEON")

    leon = Leon(
        config,
        logger,
    )
    leon.run(minimal_db_initialization)

    job = minimal_db_initialization.query(Job).filter(Job.id == job_id).first()

    print(leon.exit_code)
    assert leon.exit_code == 1
    assert job.state == "Terminated"


def test_leon_get_jobs_to_kill_running(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
    )

    FragJob.create(minimal_db_initialization, job_id=job_id, state="LEON")

    leon = Leon(
        config,
        logger,
    )
    leon.run(minimal_db_initialization)

    minimal_db_initialization.query(Job).filter(Job.id == job_id).first()

    print(leon.exit_code)
    assert leon.exit_code == 0


def test_leon_get_jobs_to_kill_running_deploy(
    minimal_db_initialization,
    setup_config,
):
    config, _, _ = setup_config
    job_id = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        properties="",
        state="Running",
        types=["deploy"],
    )

    assign_resources(minimal_db_initialization, job_id)

    FragJob.create(minimal_db_initialization, job_id=job_id, state="LEON")

    leon = Leon(
        config,
        logger,
    )
    leon.run(minimal_db_initialization)

    minimal_db_initialization.query(Job).filter(Job.id == job_id).first()

    print(leon.exit_code)
    assert leon.exit_code == 0
