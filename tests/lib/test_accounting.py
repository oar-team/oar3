# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.lib.accounting import (
    check_accounting_update,
    delete_accounting_windows_before,
    delete_all_from_accounting,
    get_accounting_summary,
    get_accounting_summary_byproject,
    get_last_project_karma,
)
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import Accounting, Job, Queue, Resource

from ..helpers import insert_running_jobs, insert_terminated_jobs


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources
        for i in range(10):
            Resource.create(session, network_address="localhost")
        Queue.create(session, name="default")

        yield session


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_check_accounting_update_one(minimal_db_initialization):
    insert_terminated_jobs(minimal_db_initialization, nb_jobs=1)
    accounting = minimal_db_initialization.query(Accounting).all()

    for a in accounting:
        print(
            a.user,
            a.project,
            a.consumption_type,
            a.queue_name,
            a.window_start,
            a.window_stop,
            a.consumption,
        )
    assert accounting[7].consumption == 172800


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_check_accounting_update(minimal_db_initialization):
    insert_terminated_jobs(
        minimal_db_initialization,
    )
    accounting = minimal_db_initialization.query(Accounting).all()
    for a in accounting:
        print(
            a.user,
            a.project,
            a.consumption_type,
            a.queue_name,
            a.window_start,
            a.window_stop,
            a.consumption,
        )

    assert accounting[7].consumption == 864000


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_delete_all_from_accounting(minimal_db_initialization):
    insert_terminated_jobs(
        minimal_db_initialization,
    )
    delete_all_from_accounting(minimal_db_initialization)
    accounting = minimal_db_initialization.query(Accounting).all()
    assert accounting == []


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_delete_accounting_windows_before(minimal_db_initialization):
    insert_terminated_jobs(
        minimal_db_initialization,
    )
    accounting1 = minimal_db_initialization.query(Accounting).all()
    delete_accounting_windows_before(minimal_db_initialization, 5 * 86400)
    accounting2 = minimal_db_initialization.query(Accounting).all()
    assert len(accounting1) > len(accounting2)


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_check_accounting_update_running_job_not_accounted(minimal_db_initialization):
    (terminated_id,) = insert_terminated_jobs(
        minimal_db_initialization, update_accounting=False, nb_jobs=1
    )
    (running_id,) = insert_running_jobs(
        minimal_db_initialization,
        nb_jobs=1,
        user="runner",
        project="runproj",
        walltime=60,
    )

    check_accounting_update(minimal_db_initialization, 86400)
    minimal_db_initialization.expire_all()

    terminated = (
        minimal_db_initialization.query(Job).filter(Job.id == terminated_id).one()
    )
    running = minimal_db_initialization.query(Job).filter(Job.id == running_id).one()
    assert terminated.accounted == "YES"
    assert running.accounted == "NO"
    assert (
        minimal_db_initialization.query(Accounting)
        .filter(Accounting.user == "runner")
        .all()
        == []
    )


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_check_accounting_update_running_job_later_accounted(minimal_db_initialization):
    (running_id,) = insert_running_jobs(
        minimal_db_initialization,
        nb_jobs=1,
        user="runner",
        project="runproj",
        walltime=60,
    )

    check_accounting_update(minimal_db_initialization, 86400)
    minimal_db_initialization.expire_all()

    running = minimal_db_initialization.query(Job).filter(Job.id == running_id).one()
    assert running.accounted == "NO"
    assert (
        minimal_db_initialization.query(Accounting)
        .filter(Accounting.user == "runner")
        .all()
        == []
    )

    # Pin start_time to a fixed value aligned inside a single accounting
    # window so the 1h USED / 60s ASKED spans never cross a window boundary
    # (using the real wall-clock via insert_running_jobs made this test flaky
    # during the last hour of the UTC day).
    minimal_db_initialization.query(Job).filter(Job.id == running_id).update(
        {Job.state: "Terminated", Job.start_time: 30000, Job.stop_time: 33600},
        synchronize_session=False,
    )
    minimal_db_initialization.commit()

    check_accounting_update(minimal_db_initialization, 86400)
    minimal_db_initialization.expire_all()

    running = minimal_db_initialization.query(Job).filter(Job.id == running_id).one()
    assert running.accounted == "YES"

    used = (
        minimal_db_initialization.query(Accounting)
        .filter(Accounting.user == "runner")
        .filter(Accounting.consumption_type == "USED")
        .all()
    )
    asked = (
        minimal_db_initialization.query(Accounting)
        .filter(Accounting.user == "runner")
        .filter(Accounting.consumption_type == "ASKED")
        .all()
    )
    assert len(used) == 1
    assert used[0].consumption == 7200
    assert len(asked) == 1
    assert asked[0].consumption == 120


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_check_accounting_update_running_jobs_survive_batch(minimal_db_initialization):
    terminated_ids = insert_terminated_jobs(
        minimal_db_initialization, update_accounting=False, nb_jobs=2
    )
    running_ids = insert_running_jobs(
        minimal_db_initialization,
        nb_jobs=2,
        user="runner",
        project="runproj",
        walltime=60,
    )

    check_accounting_update(minimal_db_initialization, 86400)
    minimal_db_initialization.expire_all()

    for job_id in terminated_ids:
        job = minimal_db_initialization.query(Job).filter(Job.id == job_id).one()
        assert job.accounted == "YES"
    for job_id in running_ids:
        job = minimal_db_initialization.query(Job).filter(Job.id == job_id).one()
        assert job.accounted == "NO"


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_check_accounting_update_idempotent(minimal_db_initialization):
    insert_terminated_jobs(minimal_db_initialization, nb_jobs=1)

    def snapshot(session):
        return sorted(
            (
                a.user,
                a.project,
                a.queue_name,
                a.consumption_type,
                a.window_start,
                a.window_stop,
                a.consumption,
            )
            for a in session.query(Accounting).all()
        )

    before = snapshot(minimal_db_initialization)
    check_accounting_update(minimal_db_initialization, 86400)
    minimal_db_initialization.expire_all()
    after = snapshot(minimal_db_initialization)
    assert before == after


def test_get_last_project_karma(minimal_db_initialization):
    user = "toto"
    project = "yopa"
    start_time = 10000
    karma = " Karma=0.345"
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        properties="",
        command="yop",
        user=user,
        project=project,
        start_time=start_time,
        message=karma,
    )
    msg1 = get_last_project_karma(minimal_db_initialization, "toto", "yopa", 50000)
    msg2 = get_last_project_karma(minimal_db_initialization, "titi", "", 50000)
    assert karma == msg1
    assert "" == msg2


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_get_accounting_summary(minimal_db_initialization):
    insert_terminated_jobs(
        minimal_db_initialization,
    )
    result1 = get_accounting_summary(minimal_db_initialization, 0, 100 * 86400)
    result2 = get_accounting_summary(minimal_db_initialization, 0, 100 * 86400, "toto")
    print(result1)
    print(result2)
    assert result1["zozo"]["USED"] == 8640000
    assert result1["zozo"]["ASKED"] == 10368000
    assert result2 == {}


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_get_accounting_summary_byproject(minimal_db_initialization):
    insert_terminated_jobs(
        minimal_db_initialization,
    )
    result1 = get_accounting_summary_byproject(
        minimal_db_initialization, 0, 100 * 86400
    )
    result2 = get_accounting_summary_byproject(
        minimal_db_initialization, 0, 100 * 86400, "toto"
    )
    print(result1)
    print(result2)
    assert result1["yopa"]["ASKED"]["zozo"] == 10368000
    assert result1["yopa"]["USED"]["zozo"] == 8640000
    assert result2 == {}
    assert True
