# coding: utf-8
from __future__ import unicode_literals, print_function

import pytest
import os

from click.testing import CliRunner

from oar.lib import db
from oar.cli.oarsub import cli

default_res = '/resource_id=1'
nodes_res = 'resource_id'


@pytest.fixture(scope="module", autouse=True)
def set_env(request):
    os.environ['OARDIR'] = '/tmp'
    os.environ['OARDO_USER'] = 'yop'


@pytest.fixture(scope="module", autouse=True)
def create_db(request):
    db.create_all()
    db.reflect()
    db.delete_all()

    @request.addfinalizer
    def teardown():
        db.delete_all()
        db.session.close()


@pytest.fixture(scope='function', autouse=True)
def minimal_db_initialization(request):

    for i in range(5):
        db['Resource'].create(network_address="localhost")

    @request.addfinalizer
    def teardown():
        db.delete_all()
        db.session.close()


def test_oarsub_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 5


def test_oarsub_sleep_1():
    runner = CliRunner()
    result = runner.invoke(cli, ['"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    mld_job_desc = db['MoldableJobDescription'].query.one()
    job_res_desc = db['JobResourceDescription'].query.one()
    print(mld_job_desc.walltime, job_res_desc.resource_type, job_res_desc.value)
    assert result.exit_code == 0
    assert job.id == 1
