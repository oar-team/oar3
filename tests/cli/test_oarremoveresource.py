# coding: utf-8
from __future__ import unicode_literals, print_function

import os
import pytest

from click.testing import CliRunner

from oar.lib import (db, Job)
from oar.cli.oarremoveresource import cli
from oar.kao.job import insert_job

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for _ in range(5):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield

def test_oarremoveresource_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 2


def test_oarremoveresource_simple():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 2

