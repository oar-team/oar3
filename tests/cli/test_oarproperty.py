# coding: utf-8
import re
from oar.lib.models import DeferredReflectionModel, Model

import pytest
from sqlalchemy import Column, Integer
from click.testing import CliRunner
from alembic.migration import MigrationContext
from alembic.operations import Operations
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
    config, _, engine = setup_config
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"], obj=(minimal_db_initialization, engine, config))
    print(result.exception)
    print(result.output)
    assert re.match(r".*\d\.\d\.\d.*", result.output)


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_add(minimal_db_initialization, setup_config):
    config, _, engine = setup_config

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["-a", "fancy", "-c"],
        catch_exceptions=True,
        obj=(minimal_db_initialization, engine, config),
    )
    print(result.output)
    assert result.exit_code == 0

    # Clean the table
    result = runner.invoke(
        cli,
        ["-d", "fancy"],
        catch_exceptions=False,
        obj=(minimal_db_initialization, engine, config),
    )

@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_simple_error(minimal_db_initialization, setup_config):
    config, _, engine = setup_config
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["-a core", "-c"],
        catch_exceptions=False,
        obj=(minimal_db_initialization, engine, config),
    )
    print(result.output)
    assert result.exit_code == 2


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_add_error1(minimal_db_initialization, setup_config):
    config, _, engine = setup_config
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["-a", "f#a:ncy"],
        catch_exceptions=False,
        obj=(minimal_db_initialization, engine, config),
    )
    print(result.output)
    assert re.match(r".*is not a valid property name.*", result.output)
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_add_error2(minimal_db_initialization, setup_config):
    config, _, engine = setup_config
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["-a", "state"],
        catch_exceptions=False,
        obj=(minimal_db_initialization, engine, config),
    )
    print(result.output)
    assert re.match(r".*OAR system property.*", result.output)
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_add_error3(minimal_db_initialization, setup_config):
    config, _, engine = setup_config
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["-a", "core"],
        catch_exceptions=False,
        obj=(minimal_db_initialization, engine, config),
    )
    print(result.output)
    assert re.match(r".*already exists.*", result.output)

    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_oarproperty_list(minimal_db_initialization, setup_config):
    config, _, engine = setup_config
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--list"],
        catch_exceptions=False,
        obj=(minimal_db_initialization, engine, config),
    )
    print(result.output)
    assert result.output.split("\n")[0] == "core"
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
@pytest.mark.skip(reason="messing up with resource table has other side effects in tests...")
def test_oarproperty_delete(minimal_db_initialization, setup_config):
    config, _, engine = setup_config
    runner = CliRunner()

    DeferredReflectionModel.prepare(engine)
    result = runner.invoke(
        cli,
        ["-d", "tadam"],
        catch_exceptions=False,
        obj=(minimal_db_initialization, engine, config),
    )
    assert result.output == "Deleted property: tadam\n"
    assert result.exit_code == 0


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
@pytest.mark.skip
def test_oarproperty_rename(minimal_db_initialization, setup_config):
    config, _, engine = setup_config
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["--rename", "tadam,madat"],
        catch_exceptions=False,
        obj=(minimal_db_initialization, engine, config),
    )

    assert result.exit_code == 0

