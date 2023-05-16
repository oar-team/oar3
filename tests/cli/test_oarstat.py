# coding: utf-8
import json
import re

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.cli.oarstat import cli
from oar.lib.database import ephemeral_session
from oar.lib.event import add_new_event
from oar.lib.job_handling import insert_job
from oar.lib.models import (
    AssignedResource,
    Job,
    MoldableJobDescription,
    Queue,
    Resource,
)

from ..helpers import insert_terminated_jobs

NB_JOBS = 5


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources
        for i in range(10):
            Resource.create(session, network_address="localhost")

        Queue.create(session, name="default")
        yield session


@pytest.fixture(scope="function")
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "get_username", lambda: "zozo")


def test_version(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"], obj=minimal_db_initialization)
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oarstat_help(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--help"], catch_exceptions=False, obj=minimal_db_initialization
    )
    print("\n" + result.output)
    # assert nb_lines == NB_JOBS + 3
    assert result.exit_code == 0


def test_oarstat_simple(minimal_db_initialization, setup_config):
    for _ in range(NB_JOBS):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
            job_user="Toto",
            message="Relatively long message",
        )

    runner = CliRunner()
    result = runner.invoke(cli, catch_exceptions=False, obj=minimal_db_initialization)
    print("\n" + result.output)
    # assert nb_lines == NB_JOBS + 3
    assert result.exit_code == 0


def test_oarstat(minimal_db_initialization, setup_config):
    for i in range(NB_JOBS):
        id = insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
            state="Running",
            job_user="Toto",
            message="Relatively long message",
        )
        assign_resources(minimal_db_initialization, id)

    for i in range(NB_JOBS):
        id = insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
            job_user="Toto",
            message="Relatively long message",
        )

    runner = CliRunner()
    result = runner.invoke(
        cli, ["-r"], catch_exceptions=False, obj=minimal_db_initialization
    )
    print("\n" + result.output)
    # assert nb_lines == NB_JOBS + 3
    assert result.exit_code == 0


def test_oarstat_simple_with_resources(minimal_db_initialization, setup_config):
    for i in range(NB_JOBS):
        id = insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
            job_user="Toto",
            message="Relatively long message",
        )
        assign_resources(minimal_db_initialization, id)

    runner = CliRunner()
    result = runner.invoke(
        cli, ["-r"], catch_exceptions=False, obj=minimal_db_initialization
    )
    print("\n" + result.output)
    # assert nb_lines == NB_JOBS + 3
    assert result.exit_code == 0


def assign_resources(session, job_id):
    moldable = (
        session.query(MoldableJobDescription)
        .filter(MoldableJobDescription.job_id == job_id)
        .first()
    )

    session.query(Job).filter(Job.id == job_id).update(
        {Job.assigned_moldable_job: moldable.id}, synchronize_session=False
    )
    resources = session.query(Resource).all()
    for r in resources[:4]:
        AssignedResource.create(session, moldable_id=moldable.id, resource_id=r.id)


def test_oarstat_full(minimal_db_initialization, setup_config):
    for i in range(NB_JOBS):
        id = insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
            job_name=f"test-{i}",
            job_user="Toto",
            command="oarsub -l ",
            message="Relatively long message",
        )
        assign_resources(minimal_db_initialization, id)

    runner = CliRunner()
    result = runner.invoke(
        cli, ["-f"], catch_exceptions=False, obj=minimal_db_initialization
    )
    print("\n" + result.output)
    # assert nb_lines == NB_JOBS + 3
    assert result.exit_code == 0


def test_oarstat_sql_property(minimal_db_initialization, setup_config):
    for i in range(NB_JOBS):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
            user=str(i),
        )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--sql", "(job_user='2' OR job_user='3')"],
        catch_exceptions=False,
        obj=minimal_db_initialization,
    )
    print("\n" + result.output)
    nb_lines = len(result.output.split("\n"))

    assert nb_lines == 7
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarstat_accounting(minimal_db_initialization, setup_config):
    insert_terminated_jobs(minimal_db_initialization)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--accounting", "1970-01-01, 1970-01-20"], obj=minimal_db_initialization, catch_exceptions=False
    )
    str_result = result.output
    print(result.exception)
    print(str_result)
    print(str_result.split("\n"))
    assert re.findall(r".*8640000.*", str_result)


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarstat_accounting_user(
    monkeypatch_tools, minimal_db_initialization, setup_config
):
    insert_terminated_jobs(minimal_db_initialization)
    karma = " Karma=0.345"
    insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=2", "")])],
        properties="",
        command="yop",
        user="zozo",
        project="yopa",
        start_time=0,
        message=karma,
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--accounting", "1970-01-01, 1970-01-20", "--user", "_this_user_"],
        obj=minimal_db_initialization, catch_exceptions=False
    )
    str_result = result.output
    print(str_result)
    print(str_result.split("\n")[-2])
    assert re.match(r".*Karma.*0.345.*", str_result.split("\n")[-2])


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarstat_accounting_error(
    monkeypatch_tools, minimal_db_initialization, setup_config
):
    insert_terminated_jobs(minimal_db_initialization)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--accounting", "1970-error, 1970-01-20"], obj=minimal_db_initialization
    )
    print(result.output)

    assert result.exit_code == 1


def test_oarstat_gantt(minimal_db_initialization, setup_config):
    insert_terminated_jobs(minimal_db_initialization, update_accounting=False)

    jobs = minimal_db_initialization.query(Job).all()
    # print_query_results(jobs)

    for j in jobs:
        pass
        # print(j.id, j.assigned_moldable_job)
    # import pdb; pdb.set_trace(# )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--gantt", "1970-01-01 01:20:00, 1970-01-20 00:00:00"],
        obj=minimal_db_initialization,
    )
    str_result = result.output
    print(str_result)
    assert re.match(".*10 days.*", str_result.split("\n")[3])


def test_oarstat_events(minimal_db_initialization, setup_config):
    job_id = insert_job(minimal_db_initialization, res=[(60, [("resource_id=4", "")])])
    add_new_event(minimal_db_initialization, "EXECUTE_JOB", job_id, "Have a good day !")
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--events", "--job", str(job_id)], obj=minimal_db_initialization
    )

    str_result = result.output.splitlines()

    print("lala: \n" + str(result))
    assert re.match(".*EXECUTE_JOB.*", str_result[3])


def test_oarstat_events_array(minimal_db_initialization, setup_config):
    job_ids = []
    for _ in range(5):
        job_id = insert_job(
            minimal_db_initialization, res=[(60, [("resource_id=4", "")])], array_id=10
        )
        add_new_event(
            minimal_db_initialization, "EXECUTE_JOB", job_id, "Have a good day !"
        )
        job_ids.append(job_id)

    runner = CliRunner()
    result = runner.invoke(
        cli, ["--events", "--array", str(10)], obj=minimal_db_initialization
    )

    print("\n" + result.output)
    # Remove the headers
    str_result = "\n".join(result.output.splitlines()[3:])

    assert re.match(".*EXECUTE_JOB.*", str_result)


def test_oarstat_events_array_json(minimal_db_initialization, setup_config):
    job_ids = []
    for _ in range(5):
        job_id = insert_job(
            minimal_db_initialization, res=[(60, [("resource_id=4", "")])], array_id=100
        )
        add_new_event(
            minimal_db_initialization, "EXECUTE_JOB", job_id, "Have a good day !"
        )
        job_ids.append(job_id)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--events", "--array", str(100), "-J"],
        catch_exceptions=False,
        obj=minimal_db_initialization,
    )

    print("lla\n" + result.output)
    try:
        parsed_json = json.loads(result.output)
        assert len(parsed_json) == 5
    except ValueError:
        assert False
    assert result.exit_code == 0


def test_oarstat_events_no_job_ids(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--events", "--array", str(20)], obj=minimal_db_initialization
    )
    str_result = result.output
    print(str_result)
    assert re.match(".*No job ids specified.*", str_result)


def test_oarstat_properties(minimal_db_initialization, setup_config):
    insert_terminated_jobs(minimal_db_initialization, update_accounting=False)
    job_id = minimal_db_initialization.query(Job.id).first()[0]
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--properties", "--job", str(job_id)],
        catch_exceptions=False,
        obj=minimal_db_initialization,
    )
    str_result = result.output
    print("res:\n" + str_result)
    assert re.match(".*network_address.*", str_result)


def test_oarstat_properties_json(minimal_db_initialization, setup_config):
    insert_terminated_jobs(minimal_db_initialization, update_accounting=False)
    job_id = minimal_db_initialization.query(Job.id).first()[0]
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--properties", "--job", str(job_id), "-J"],
        catch_exceptions=False,
        obj=minimal_db_initialization,
    )
    parsed_json = json.loads(result.output)
    print(parsed_json)
    assert str(job_id) in parsed_json
    assert len(parsed_json[str(job_id)]) == 2


def test_oarstat_state(minimal_db_initialization, setup_config):
    job_id = insert_job(minimal_db_initialization, res=[(60, [("resource_id=2", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--state", "--job", str(job_id)], obj=minimal_db_initialization
    )
    str_result = result.output
    print(str_result)
    assert re.match(".*Waiting.*", str_result)


def test_oarstat_state_json(minimal_db_initialization, setup_config):
    job_id = insert_job(minimal_db_initialization, res=[(60, [("resource_id=2", "")])])
    job_id1 = insert_job(minimal_db_initialization, res=[(60, [("resource_id=2", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--state", "--job", str(job_id), "--job", str(job_id1), "--json"],
        obj=minimal_db_initialization,
    )
    str_result = result.output
    try:
        parsed_json = json.loads(str_result)
        assert len(parsed_json) == 2
    except ValueError:
        assert False
    assert result.exit_code == 0


def test_oarstat_simple_json(minimal_db_initialization, setup_config):
    for _ in range(NB_JOBS):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            properties="",
        )
    runner = CliRunner()
    result = runner.invoke(cli, ["--json"], obj=minimal_db_initialization)
    str_result = result.output
    print(str_result)
    try:
        parsed_json = json.loads(str_result)
        assert len(parsed_json) == NB_JOBS
    except ValueError:
        assert False
    assert result.exit_code == 0


def test_oarstat_full_json(minimal_db_initialization, setup_config):
    for _ in range(NB_JOBS):
        insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            user="toto",
            properties="",
        )

    runner = CliRunner()
    result = runner.invoke(cli, ["--json", "--full"], obj=minimal_db_initialization)
    str_result = result.output
    print(str_result)
    try:
        print(str_result)
        parsed_json = json.loads(str_result)
        assert len(parsed_json) == NB_JOBS
        for job in parsed_json:
            assert "cpuset_name" in parsed_json[job]

    except ValueError:
        assert False
    assert result.exit_code == 0


def test_oarstat_json_only_one_job(minimal_db_initialization, setup_config):
    for _ in range(NB_JOBS):
        jid = insert_job(
            minimal_db_initialization,
            res=[(60, [("resource_id=4", "")])],
            user="toto",
            properties="",
        )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--json", "--full", "-j", str(jid), "-j", str(jid - 1)],
        obj=minimal_db_initialization,
    )
    str_result = result.output

    try:
        parsed_json = json.loads(str_result)
        print(parsed_json)
        assert len(parsed_json) == 2
        assert parsed_json[str(jid)]["id"] == jid
    except ValueError:
        assert False
    assert result.exit_code == 0


def test_oarstat_job_id_array_error(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-j", "1", "--array", "1"], obj=minimal_db_initialization
    )
    print(result.output)
    assert result.exit_code == 1


def test_oarstat_job_id_error(minimal_db_initialization, setup_config):
    # Error jobs
    jid = insert_job(
        minimal_db_initialization,
        res=[(60, [("resource_id=4", "")])],
        user="toto",
        properties="",
        state="Error",
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["-j", str(jid), "-J"], obj=minimal_db_initialization)

    print(result.output)
    str_result = result.output

    try:
        parsed_json = json.loads(str_result)
        print(parsed_json)
        assert len(parsed_json) == 1
        assert parsed_json[str(jid)]["id"] == jid

    except ValueError:
        assert False
    assert result.exit_code == 0
