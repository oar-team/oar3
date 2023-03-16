# coding: utf-8
import json
import re

import pytest
from click.testing import CliRunner

import oar.lib.tools  # for monkeypatching
from oar.cli.oarstat import cli
from oar.lib import Job, db
from oar.lib.event import add_new_event
from oar.lib.job_handling import insert_job
from oar.lib.utils import print_query_results
from oar.lib import (
    AssignedResource,
    FragJob,
    Job,
    MoldableJobDescription,
    Resource,
    config,
    db,
)
from ..helpers import insert_terminated_jobs

NB_JOBS = 5


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(10):
            db["Resource"].create(network_address="localhost")

        db["Queue"].create(name="default")
        yield


@pytest.fixture(scope="function")
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "get_username", lambda: "zozo")


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"])
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oarstat_simple():
    for _ in range(NB_JOBS):
        insert_job(
            res=[(60, [("resource_id=4", "")])],
            properties="",
            job_user="Toto",
            message="Relatively long message",
        )

    runner = CliRunner()
    result = runner.invoke(cli, catch_exceptions=False)
    nb_lines = len(result.output.split("\n"))
    print("\n" + result.output)
    # assert nb_lines == NB_JOBS + 3
    assert result.exit_code == 0


def assign_resources(job_id):
    moldable = (
        db.query(MoldableJobDescription)
        .filter(MoldableJobDescription.job_id == job_id)
        .first()
    )

    db.query(Job).filter(Job.id == job_id).update(
        {Job.assigned_moldable_job: moldable.id}, synchronize_session=False
    )
    resources = db.query(Resource).all()
    for r in resources[:4]:
        AssignedResource.create(moldable_id=moldable.id, resource_id=r.id)


def test_oarstat_full():
    for i in range(NB_JOBS):
        id = insert_job(
            res=[(60, [("resource_id=4", "")])],
            properties="",
            job_name=f"test-{i}",
            job_user="Toto",
            command="oarsub -l ",
            message="Relatively long message",
        )
        assign_resources(id)

    runner = CliRunner()
    result = runner.invoke(cli, ["-f", "-J"],catch_exceptions=False)
    nb_lines = len(result.output.split("\n"))
    print("\n" + result.output)
    # assert nb_lines == NB_JOBS + 3
    assert result.exit_code == 0


def test_oarstat_sql_property():
    for i in range(NB_JOBS):
        insert_job(res=[(60, [("resource_id=4", "")])], properties="", user=str(i))
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--sql", "(job_user='2' OR job_user='3')"], catch_exceptions=False
    )
    print("\n" + result.output)
    nb_lines = len(result.output.split("\n"))

    assert nb_lines == 5
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarstat_accounting():
    insert_terminated_jobs()
    runner = CliRunner()
    result = runner.invoke(cli, ["--accounting", "1970-01-01, 1970-01-20"])
    str_result = result.output
    print(str_result)
    print(str_result.split("\n"))
    assert re.match(r".*8640000.*", str_result.split("\n")[2])


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarstat_accounting_user(monkeypatch_tools):
    insert_terminated_jobs()
    karma = " Karma=0.345"
    insert_job(
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
        cli, ["--accounting", "1970-01-01, 1970-01-20", "--user", "_this_user_"]
    )
    str_result = result.output
    print(str_result)
    print(str_result.split("\n")[-2])
    assert re.match(r".*Karma.*0.345.*", str_result.split("\n")[-2])


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarstat_accounting_error(monkeypatch_tools):
    insert_terminated_jobs()
    runner = CliRunner()
    result = runner.invoke(cli, ["--accounting", "1970-error, 1970-01-20"])
    print(result.output)

    assert result.exit_code == 1


def test_oarstat_gantt():
    insert_terminated_jobs(update_accounting=False)

    jobs = db.query(Job).all()
    # print_query_results(jobs)

    for j in jobs:
        pass
        # print(j.id, j.assigned_moldable_job)
    # import pdb; pdb.set_trace(# )
    runner = CliRunner()
    result = runner.invoke(cli, ["--gantt", "1970-01-01 01:20:00, 1970-01-20 00:00:00"])
    str_result = result.output
    print(str_result)
    assert re.match(".*10 days.*", str_result.split("\n")[3])


def test_oarstat_events():
    job_id = insert_job(res=[(60, [("resource_id=4", "")])])
    add_new_event("EXECUTE_JOB", job_id, "Have a good day !")

    runner = CliRunner()
    result = runner.invoke(cli, ["--events", "--job", str(job_id)])

    str_result = result.output.splitlines()
    print("\n" + result.output)
    assert re.match(".*EXECUTE_JOB.*", str_result[2])


def test_oarstat_events_array():
    job_ids = []
    for _ in range(5):
        job_id = insert_job(res=[(60, [("resource_id=4", "")])], array_id=10)
        add_new_event("EXECUTE_JOB", job_id, "Have a good day !")
        job_ids.append(job_id)

    runner = CliRunner()
    result = runner.invoke(cli, ["--events", "--array", str(10)])

    print("\n" + result.output)
    # Remove the headers
    str_result = "\n".join(result.output.splitlines()[2:])

    assert re.match(".*EXECUTE_JOB.*", str_result)


def test_oarstat_events_array_json():
    job_ids = []
    for _ in range(5):
        job_id = insert_job(res=[(60, [("resource_id=4", "")])], array_id=100)
        add_new_event("EXECUTE_JOB", job_id, "Have a good day !")
        job_ids.append(job_id)

    runner = CliRunner()
    result = runner.invoke(
        cli, ["--events", "--array", str(100), "-J"], catch_exceptions=False
    )

    print("lla\n" + result.output)
    try:
        parsed_json = json.loads(result.output)
        assert len(parsed_json) == 5
    except ValueError:
        assert False
    assert result.exit_code == 0


def test_oarstat_events_no_job_ids():
    runner = CliRunner()
    result = runner.invoke(cli, ["--events", "--array", str(20)])
    str_result = result.output
    print(str_result)
    assert re.match(".*No job ids specified.*", str_result)


def test_oarstat_properties():
    insert_terminated_jobs(update_accounting=False)
    job_id = db.query(Job.id).first()[0]
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--properties", "--job", str(job_id)], catch_exceptions=False
    )
    str_result = result.output
    print("res:\n" + str_result)
    assert re.match(".*network_address.*", str_result)


def test_oarstat_properties_json():
    insert_terminated_jobs(update_accounting=False)
    job_id = db.query(Job.id).first()[0]
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--properties", "--job", str(job_id), "-J"], catch_exceptions=False
    )
    parsed_json = json.loads(result.output)
    print(parsed_json)
    assert str(job_id) in parsed_json
    assert len(parsed_json[str(job_id)]) == 2


def test_oarstat_state():
    job_id = insert_job(res=[(60, [("resource_id=2", "")])])
    runner = CliRunner()
    result = runner.invoke(cli, ["--state", "--job", str(job_id)])
    str_result = result.output
    print(str_result)
    assert re.match(".*Waiting.*", str_result)


def test_oarstat_state_json():
    job_id = insert_job(res=[(60, [("resource_id=2", "")])])
    job_id1 = insert_job(res=[(60, [("resource_id=2", "")])])
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--state", "--job", str(job_id), "--job", str(job_id1), "--json"]
    )
    str_result = result.output
    try:
        parsed_json = json.loads(str_result)
        assert len(parsed_json) == 2
    except ValueError:
        assert False
    assert result.exit_code == 0


def test_oarstat_simple_json():
    for _ in range(NB_JOBS):
        insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli, ["--json"])
    str_result = result.output
    print(str_result)
    try:
        parsed_json = json.loads(str_result)
        assert len(parsed_json) == NB_JOBS
    except ValueError:
        assert False
    assert result.exit_code == 0


def test_oarstat_full_json():
    for _ in range(NB_JOBS):
        insert_job(res=[(60, [("resource_id=4", "")])], user="toto", properties="")

    runner = CliRunner()
    result = runner.invoke(cli, ["--json", "--full"])
    str_result = result.output
    print(str_result)
    try:
        parsed_json = json.loads(str_result)
        assert len(parsed_json) == NB_JOBS
        for job in parsed_json:
            assert "cpuset_name" in parsed_json[job]

    except ValueError:
        assert False
    assert result.exit_code == 0


def test_oarstat_json_only_one_job():
    for _ in range(NB_JOBS):
        jid = insert_job(
            res=[(60, [("resource_id=4", "")])], user="toto", properties=""
        )

    runner = CliRunner()
    result = runner.invoke(
        cli, ["--json", "--full", "-j", str(jid), "-j", str(jid - 1)]
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


def test_oarstat_job_id_array_error():
    runner = CliRunner()
    result = runner.invoke(cli, ["-j", "1", "--array", "1"])
    print(result.output)
    assert result.exit_code == 1


def test_oarstat_job_id_error():
    # Error jobs
    jid = insert_job(
        res=[(60, [("resource_id=4", "")])], user="toto", properties="", state="Error"
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["-j", str(jid), "-J"])

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
