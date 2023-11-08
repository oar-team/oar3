# coding: utf-8
import json
import re

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.cli.oarnodes import cli
from oar.lib.database import ephemeral_session
from oar.lib.event import add_new_event_with_host
from oar.lib.models import Resource

NB_NODES = 5
NB_LINES_PER_NODE = 1  # network_address: localhost\n resource_id: 1\n state: Alive\n


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)
    with ephemeral_session(scoped, engine, bind=engine) as session:
        for i in range(NB_NODES):
            Resource.create(session, network_address="localhost")

        yield session


def test_version(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"], obj=(minimal_db_initialization, config))
    print(result.output)
    print(result.exception)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


def test_oarnodes_event_no_date(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    add_new_event_with_host(
        minimal_db_initialization, "TEST", 1, "fake_event", ["localhost"]
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--events", "_events_without_date_"],
        obj=(minimal_db_initialization, config),
    )
    print(result.output)
    print(result.exception)
    assert re.findall(r".*fake_event.*", result.output)


def test_oarnodes_event(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    add_new_event_with_host(
        minimal_db_initialization, "TEST", 1, "fake_event", ["localhost"]
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--events", "1970-01-01 01:20:00"],
        obj=(minimal_db_initialization, config),
    )
    print(result)
    print("\n" + result.output)
    assert re.findall(r".*fake_event.*", result.output)


def test_oarnodes_event_json(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    add_new_event_with_host(
        minimal_db_initialization, "TEST", 1, "fake_event", ["localhost"]
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--events", "1970-01-01 01:20:00", "--json"],
        obj=(minimal_db_initialization, config),
    )
    data = json.loads(result.output)
    assert re.match(r".*fake_event.*", data["localhost"][0]["description"])


def test_oarnodes_resource_ids_state(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    rid = [r[0] for r in minimal_db_initialization.query(Resource.id).all()]
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--state", "-r", str(rid[0]), "-r", str(rid[1])],
        obj=(minimal_db_initialization, config),
    )
    print(result.output)
    assert re.match(r".*Alive.*", result.output)


def test_oarnodes_resource_ids_state_json(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    rid = [r[0] for r in minimal_db_initialization.query(Resource.id).all()]
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--state", "-r", str(rid[0]), "-r", str(rid[1]), "--json"],
        obj=(minimal_db_initialization, config),
    )
    print(result.output)
    assert re.match(r".*Alive.*", result.output)


def test_oarnodes_hosts_state(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    Resource.create(minimal_db_initialization, network_address="akira", state="Absent")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli, ["--state", "localhost", "akira"], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    assert len(result.output.split("\n")) == 8


def test_oarnodes_hosts_state_json(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    Resource.create(minimal_db_initialization, network_address="akira", state="Absent")
    Resource.create(
        minimal_db_initialization,
        network_address="akira",
        state="Absent",
        available_upto=10,
    )
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--state", "localhost", "akira", "--json"],
        obj=(minimal_db_initialization, config),
    )
    print(result.output)
    assert re.match(r".*Standby.*", result.output)
    assert re.match(r".*Absent.*", result.output)


def test_oarnodes_list_state(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    Resource.create(minimal_db_initialization, network_address="akira")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ["-l"], obj=(minimal_db_initialization, config))
    print(result.output)
    assert len(result.output.split("\n")) == 3


def test_oarnodes_list_state_json(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    Resource.create(minimal_db_initialization, network_address="akira")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli, ["-l", "--json"], obj=(minimal_db_initialization, config)
    )
    print(result.output)
    assert re.match(r".*localhost.*", result.output)
    assert re.match(r".*akira.*", result.output)


def test_oarnodes_simple(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, obj=(minimal_db_initialization, config))
    lines = re.findall(r".*localhost.*", result.output)
    assert len(lines) == NB_LINES_PER_NODE * NB_NODES  # + 1 for last \n
    assert result.exit_code == 0


def test_oarnodes_simple_json(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["--json"], obj=(minimal_db_initialization, config))
    print(result.output)
    assert re.match(r".*localhost.*", result.output)
    assert result.exit_code == 0


def test_oarnodes_sql(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    for _ in range(2):
        Resource.create(minimal_db_initialization, network_address="akira")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--sql", "network_address='akira'"],
        obj=(minimal_db_initialization, config),
    )
    print(result.exception)
    concerned_lines = re.findall(r".*akira.*", result.output)
    assert len(concerned_lines) == 2
    assert result.exit_code == 0


def test_oarnodes_sql_json(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    for _ in range(2):
        Resource.create(minimal_db_initialization, network_address="akira")
    minimal_db_initialization.commit()
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--sql", "network_address='akira'", "--json"],
        obj=(minimal_db_initialization, config),
    )
    data = json.loads(result.output)
    assert len(data) == 2
    assert result.exit_code == 0
