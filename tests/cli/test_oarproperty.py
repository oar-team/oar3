# coding: utf-8
import re

import pytest
from click.testing import CliRunner
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.cli.oarproperty import cli
from oar.lib.database import ephemeral_session


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        yield session


def test_version(minimal_db_initialization, setup_config):
    config, _, _ = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"], obj=(minimal_db_initialization, config))
    print(result.exception)
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_add(minimal_db_initialization, setup_config):
    config, engine, log = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-a", "fancy", "-c"], catch_exceptions=False, obj=(minimal_db_initialization, config))
    print(result.output)
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_simple_error(minimal_db_initialization, setup_config):
    config, engine, log = setup_config
    runner = CliRunner()

    result = runner.invoke(cli, ["-a core", "-c"], catch_exceptions=False, obj=(minimal_db_initialization, config))
    print(result.output)
    assert result.exit_code == 2


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_add_error1(minimal_db_initialization, setup_config):
    config, engine, log = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-a", "f#a:ncy"], catch_exceptions=False, obj=(minimal_db_initialization, config))
    print(result.output)
    assert re.match(r".*is not a valid property name.*", result.output)
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_add_error2(minimal_db_initialization, setup_config):
    config, engine, log = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-a", "state"], catch_exceptions=False, obj=(minimal_db_initialization, config))
    print(result.output)
    assert re.match(r".*OAR system property.*", result.output)
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_add_error3(minimal_db_initialization, setup_config):
    config, engine, log = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-a", "core"], catch_exceptions=False, obj=(minimal_db_initialization, config))
    print(result.output)
    assert re.match(r".*already exists.*", result.output)

    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_list(minimal_db_initialization, setup_config):
    config, engine, log = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["--list"], catch_exceptions=False, obj=(minimal_db_initialization, config))
    print(result.output)
    assert result.output.split("\n")[0] == "core"
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_delete(minimal_db_initialization, setup_config):
    config, engine, log = setup_config
    # column_name1 = [p.name for p in db["resources"].columns]
    runner = CliRunner()
    result = runner.invoke(cli, ["-d", "core"], catch_exceptions=False, obj=(minimal_db_initialization, config))
    print(result.output)
    # column_name2 = [p.name for p in db["resources"].columns]
    # assert 'core' in db['resources'].columns
    assert result.exit_code == 0
    # assert len(column_name1) == len(column_name2) + 1
    kw = {"nullable": True}
    # db.op.add_column("resources", db.Column("core", db.Integer, **kw))


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_rename(minimal_db_initialization, setup_config):
    config, engine, log = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["--rename", "core,eroc"], catch_exceptions=False, obj=(minimal_db_initialization, config))
    print(result.output)
    # assert 'eroc' in [p.name for p in db['resources'].columns]
    assert result.exit_code == 0
    # db.op.alter_column("resources", "eroc", new_column_name="core")
