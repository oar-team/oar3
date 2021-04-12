# coding: utf-8
import os

import pytest
from click.testing import CliRunner

from oar.cli.oarremoveresource import cli
from oar.lib import Resource, db


@pytest.yield_fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for _ in range(5):
            db["Resource"].create(network_address="localhost")

        db["Queue"].create(name="default")
        yield


def test_oarremoveresource_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 2


def test_oarremoveresource_bad_user():
    os.environ["OARDO_USER"] = "Zorglub"
    runner = CliRunner()
    result = runner.invoke(cli, ["1"])
    assert result.exit_code == 4


def test_oarremoveresource_not_dead():
    os.environ["OARDO_USER"] = "oar"
    first_id = db.query(Resource).first().id
    runner = CliRunner()
    result = runner.invoke(cli, [str(first_id)])
    assert result.exit_code == 3


def test_oarremoveresource_no_resource():
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 2


def test_oarremoveresource_simple():
    os.environ["OARDO_USER"] = "oar"
    runner = CliRunner()
    db["Resource"].create(network_address="localhost", state="Dead")
    nb_res1 = len(db.query(Resource).all())
    first_id = db.query(Resource).first().id
    dead_rid = first_id + 5

    result = runner.invoke(cli, [str(dead_rid)])
    nb_res2 = len(db.query(Resource).all())
    assert nb_res1 == 6
    assert nb_res2 == 5
    assert result.exit_code == 0
