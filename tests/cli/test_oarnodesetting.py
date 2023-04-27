# coding: utf-8
import os
import re
import time

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.cli.oarnodesetting import cli
from oar.lib.database import ephemeral_session
from oar.lib.models import FragJob, JobStateLog, Queue, Resource

from ..helpers import insert_running_jobs

fake_notifications = []


def fake_notify_almighty(notification):
    global fake_notifications
    fake_notifications.append(notification)


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch, minimal_db_initialization, setup_config):
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", fake_notify_almighty)
    monkeypatch.setattr(time, "sleep", lambda x: True)


def test_version(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"])
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oarnodesetting_simple(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(cli, ["-a"])
    resource = minimal_db_initialization.query(Resource).one()
    print(resource)
    # nb_lines = len(result.output_bytes.decode().split('\n'))
    # assert nb_lines == NB_LINES_PER_NODE * NB_NODES + 1 # + 1 for last \n
    assert resource.state == "Alive"
    assert fake_notifications[-2:] == ["ChState", "Term"]
    assert result.exit_code == 0


def test_oarnodesetting_core_cpu(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(cli, ["-a", "-p core=1", "-p cpu=2"])
    print(result)
    resource = minimal_db_initialization.query(Resource).one()
    print(resource)
    print(fake_notifications)
    # import pdb; pdb.set_trace()
    assert resource.core == 1
    assert resource.cpu == 2
    assert fake_notifications[-3:] == ["Term", "ChState", "Term"]
    assert result.exit_code == 0


def test_oarnodesetting_error_1(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(cli, ["-r", "1"])
    print(result.output)
    assert result.exit_code == 1


def test_oarnodesetting_error_2(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(cli, ["-r", "1", "--state", "Suspected"])
    print(result.output)
    assert result.exit_code == 1


def test_oarnodesetting_error_3(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(cli, ["-r", "1", "--maintenance", "midoff"])
    print(result.output)
    assert result.exit_code == 1


def test_oarnodesetting_error_4(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(cli, ["-r", "1", "--drain", "midoff"])
    print(result.output)
    assert result.exit_code == 1


def test_oarnodesetting_error_5(minimal_db_initialization, setup_config):
    runner = CliRunner()
    result = runner.invoke(cli, ["-r", "1", "--add"])
    print(result.output)
    assert result.exit_code == 1


def test_oarnodesetting_sql_drain(minimal_db_initialization, setup_config):
    Resource.create(minimal_db_initialization, network_address="localhost")
    runner = CliRunner()
    result = runner.invoke(cli, ["--sql", "state='Alive'", "--drain", "on"])
    resource = minimal_db_initialization.query(Resource).one()
    print(result.output)
    assert resource.drain == "YES"


def test_oarnodesetting_drain_off(minimal_db_initialization, setup_config):
    Resource.create(minimal_db_initialization, network_address="localhost", drain="YES")
    resource = minimal_db_initialization.query(Resource).one()
    print(resource.drain)
    runner = CliRunner()
    result = runner.invoke(cli, ["-h", "localhost", "--drain", "off"])
    resource = minimal_db_initialization.query(Resource).one()
    print(result.output)
    assert resource.drain == "NO"


def test_oarnodesetting_sql_void(minimal_db_initialization, setup_config):
    Resource.create(minimal_db_initialization, network_address="localhost")
    runner = CliRunner()
    result = runner.invoke(cli, ["--sql", "state='NotExist'", "--drain", "on"])
    print(result.output)
    assert re.match(".*are no resource.*", result.output)
    assert result.exit_code == 0


def test_oarnodesetting_system_property_error(minimal_db_initialization, setup_config):
    Resource.create(minimal_db_initialization, network_address="localhost")
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-h", "localhost", "-p", "state=Alive", "--drain", "on"]
    )
    print(result.output)
    assert result.exit_code == 8


def test_oarnodesetting_malformed_property_error(
    minimal_db_initialization, setup_config
):
    Resource.create(minimal_db_initialization, network_address="localhost")
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-h", "localhost", "-p", "state=Ali=ve", "--drain", "on"]
    )
    print(result.output)
    assert result.exit_code == 10


def test_oarnodesetting_sql_state(minimal_db_initialization, setup_config):
    for _ in range(2):
        Resource.create(
            minimal_db_initialization, network_address="localhost", state="Absent"
        )
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--sql", "network_address='localhost'", "--state", "Alive"]
    )
    assert fake_notifications == ["ChState"]
    assert result.exit_code == 0


def test_oarnodesetting_sql_state1(minimal_db_initialization, setup_config):
    for _ in range(2):
        Resource.create(minimal_db_initialization, network_address="localhost")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--sql", "network_address='localhost'", "--state", "Dead"]
    )
    assert fake_notifications == ["ChState"]
    assert result.exit_code == 0


def test_oarnodesetting_host_by_file_state(minimal_db_initialization, setup_config):
    here = os.path.abspath(os.path.dirname(__file__))
    filename = os.path.join(here, "data", "hostfile.txt")
    for _ in range(2):
        Resource.create(minimal_db_initialization, network_address="localhost")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["--file", filename, "--state", "Absent"])
    assert fake_notifications == ["ChState"]
    assert result.exit_code == 0


def test_oarnodesetting_hosts_state(minimal_db_initialization, setup_config):
    Resource.create(
        minimal_db_initialization, network_address="localhost0", state="Absent"
    )
    Resource.create(
        minimal_db_initialization, network_address="localhost1", state="Absent"
    )
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-h", "localhost0", "-h", "localhost1", "--state", "Alive"]
    )
    print(result.output)
    resources = minimal_db_initialization.query(Resource).all()
    assert resources[0].next_state == "Alive"
    assert fake_notifications == ["ChState"]
    assert result.exit_code == 0


def test_oarnodesetting_hosts_state1(minimal_db_initialization, setup_config):
    Resource.create(
        minimal_db_initialization, network_address="localhost0", state="Absent"
    )
    Resource.create(
        minimal_db_initialization, network_address="localhost1", state="Absent"
    )
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-h", "localhost0", "-h", "localhost1", "--state", "Dead"]
    )
    print(result.output)
    resources = minimal_db_initialization.query(Resource).all()
    assert resources[0].next_state == "Dead"
    assert fake_notifications == ["ChState"]
    assert result.exit_code == 0


def test_oarnodesetting_last_property_value(minimal_db_initialization, setup_config):
    Resource.create(minimal_db_initialization, network_address="localhost", core="1")
    Resource.create(minimal_db_initialization, network_address="localhost", core="2")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["--last-property-value", "core"])
    print(result.output)
    assert re.match(r"2", result.output)


def test_oarnodesetting_last_property_value_error0(
    minimal_db_initialization, setup_config
):
    Resource.create(minimal_db_initialization, network_address="localhost")
    Resource.create(minimal_db_initialization, network_address="localhost")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["--last-property-value", "NotExist"])
    print(result.output)
    assert re.match(r".*retrieve the last value.*", result.output)


def todo_test_oarnodesetting_last_property_value_error1(
    minimal_db_initialization, setup_config
):
    Resource.create(minimal_db_initialization, network_address="localhost")
    Resource.create(minimal_db_initialization, network_address="localhost")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["--last-property-value", "drain"])
    print(result.output)
    assert re.match(r".*retrieve the last value.*", result.output)


def test_oarnodesetting_maintenance_on_nowait(minimal_db_initialization, setup_config):
    Resource.create(minimal_db_initialization, network_address="localhost")
    minimal_db_initialization.commit()
    last_available_upto = (
        minimal_db_initialization.query(Resource).one().last_available_upto
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["-h", "localhost", "--maintenance", "on", "--no-wait"])
    print(result.output)
    print(fake_notifications)
    assert fake_notifications == ["ChState"]
    assert (
        last_available_upto
        != minimal_db_initialization.query(Resource).one().last_available_upto
    )


def test_oarnodesetting_maintenance_off_nowait(minimal_db_initialization, setup_config):
    Resource.create(
        minimal_db_initialization,
        network_address="localhost",
        last_available_upto=12334567,
    )
    minimal_db_initialization.commit()
    last_available_upto = minimal_db_initialization.query(Resource).one().available_upto
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-h", "localhost", "--maintenance", "off", "--no-wait"]
    )
    print(result.output)
    print(fake_notifications)
    assert fake_notifications == ["ChState"]
    assert (
        last_available_upto
        != minimal_db_initialization.query(Resource).one().available_upto
    )


def test_oarnodesetting_maintenance_on_wait_timeout(
    minimal_db_initialization, setup_config
):
    config, _, _ = setup_config
    for _ in range(10):
        Resource.create(minimal_db_initialization, network_address="localhost")
    minimal_db_initialization.commit()
    insert_running_jobs(minimal_db_initialization)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["-h", "localhost", "--maintenance", "on"],
        obj=(minimal_db_initialization, config),
    )
    print(result.output)
    assert result.exit_code == 11
    # print(fake_notifications)
    # assert len(fake_notifications) == 10
    # resources = db['Resource'].query.order_by(Resource.id).all()
    # assert last_available_upto != resources[0].last_available_upto
