# coding: utf-8
import re

import pytest
from click.testing import CliRunner

from oar.cli.oarnodes import cli
from oar.lib import Resource, db
from oar.lib.event import add_new_event_with_host

NB_NODES = 5
NB_LINES_PER_NODE = 4  # network_address: localhost\n resource_id: 1\n state: Alive\n


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(NB_NODES):
            db["Resource"].create(network_address="localhost")
        yield


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"])
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oarnodes_event_no_date():
    add_new_event_with_host("TEST", 1, "fake_event", ["localhost"])

    runner = CliRunner()
    result = runner.invoke(cli, ["--events", "_events_without_date_"])
    print(result.output)
    assert re.match(r".*fake_event.*", result.output)


def test_oarnodes_event():
    add_new_event_with_host("TEST", 1, "fake_event", ["localhost"])

    runner = CliRunner()
    result = runner.invoke(cli, ["--events", "1970-01-01 01:20:00"])
    print(result)
    print("\n"+result.output)
    assert re.findall(r".*fake_event.*", result.output)


def test_oarnodes_event_json():
    add_new_event_with_host("TEST", 1, "fake_event", ["localhost"])
    runner = CliRunner()
    result = runner.invoke(cli, ["--events", "1970-01-01 01:20:00", "--json"])
    print(result.output)
    assert re.match(r".*fake_event.*", result.output)


def test_oarnodes_resource_ids_state():
    rid = [r[0] for r in db.query(Resource.id).all()]
    runner = CliRunner()
    result = runner.invoke(cli, ["--state", "-r", str(rid[0]), "-r", str(rid[1])])
    print(result.output)
    assert re.match(r".*Alive.*", result.output)


def test_oarnodes_resource_ids_state_json():
    rid = [r[0] for r in db.query(Resource.id).all()]
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--state", "-r", str(rid[0]), "-r", str(rid[1]), "--json"]
    )
    print(result.output)
    assert re.match(r".*Alive.*", result.output)


def test_oarnodes_hosts_state():
    db["Resource"].create(network_address="akira", state="Absent")
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["--state", "localhost", "akira"])
    print(result.output)
    assert len(result.output.split("\n")) == 9


def test_oarnodes_hosts_state_json():
    db["Resource"].create(network_address="akira", state="Absent")
    db["Resource"].create(network_address="akira", state="Absent", available_upto=10)
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["--state", "localhost", "akira", "--json"])
    print(result.output)
    assert re.match(r".*Standby.*", result.output)
    assert re.match(r".*Absent.*", result.output)


def test_oarnodes_list_state():
    db["Resource"].create(network_address="akira")
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["-l"])
    print(result.output)
    assert len(result.output.split("\n")) == 3


def test_oarnodes_list_state_json():
    db["Resource"].create(network_address="akira")
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["-l", "--json"])
    print(result.output)
    assert re.match(r".*localhost.*", result.output)
    assert re.match(r".*akira.*", result.output)


def test_oarnodes_simple():
    runner = CliRunner()
    result = runner.invoke(cli)
    nb_lines = len(result.output.split("\n"))
    print(result.output)
    assert nb_lines == NB_LINES_PER_NODE * NB_NODES + 1  # + 1 for last \n
    assert result.exit_code == 0


def test_oarnodes_simple_json():
    runner = CliRunner()
    result = runner.invoke(cli, ["--json"])
    print(result.output)
    assert re.match(r".*localhost.*", result.output)
    assert result.exit_code == 0


def test_oarnodes_sql():
    for _ in range(2):
        db["Resource"].create(network_address="akira")
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["--sql", "network_address='akira'"])
    print(result.output)
    nb_lines = len(result.output.split("\n"))
    assert nb_lines == 2 * 4 + 1
    assert result.exit_code == 0


def test_oarnodes_sql_json():
    for _ in range(2):
        db["Resource"].create(network_address="akira")
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["--sql", "network_address='akira'", "--json"])
    print(result.output)
    assert re.match(r".*akira.*", result.output)
    assert result.exit_code == 0
