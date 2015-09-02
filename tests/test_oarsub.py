# coding: utf-8
from __future__ import unicode_literals, print_function

import pytest
import os

from click.testing import CliRunner

from oar.lib import db
from oar.cli.oarsub import cli

import oar.kao.utils  # for monkeypatching

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

    db['Queue'].create(name='default')

    @request.addfinalizer
    def teardown():
        db.delete_all()
        db.session.close()


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_utils(request, monkeypatch):
    monkeypatch.setattr(oar.kao.utils, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.kao.utils, 'notify_almighty', lambda x: len(x))


def test_oarsub_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 5


def test_oarsub_sleep_1(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '"sleep 1"'])
    print(result.output)
    # job = db['Job'].query.one()
    mld_job_desc = db['MoldableJobDescription'].query.one()
    job_res_desc = db['JobResourceDescription'].query.one()
    print(mld_job_desc.walltime, job_res_desc.resource_type, job_res_desc.value)
    assert result.exit_code == 0
    assert mld_job_desc.walltime == 0
    assert job_res_desc.resource_type == 'resource_id'
    assert job_res_desc.value == 1


def test_oarsub_sleep_2(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-l resource_id=3', '-q default', '"sleep 1"'])
    print(result.output)
    # job = db['Job'].query.one()
    mld_job_desc = db['MoldableJobDescription'].query.one()
    job_res_desc = db['JobResourceDescription'].query.one()
    print(mld_job_desc.walltime, job_res_desc.resource_type, job_res_desc.value)
    assert result.exit_code == 0
    assert mld_job_desc.walltime == 0
    assert job_res_desc.resource_type == 'resource_id'
    assert job_res_desc.value == 3


def test_oarsub_admission_name_1(monkeypatch):

    db['AdmissionRule'].create(rule="name='yop'")
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    print("name: ", job.name)
    assert result.exit_code == 0
    assert job.name == 'yop'


def test_oarsub_admission_queue_1(monkeypatch):

    db['AdmissionRule'].create(rule=("if user == 'yop':"
                                     "    queue_name= 'default'"))

    runner = CliRunner()
    result = runner.invoke(cli, ['-q noexist', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    print("queue-name: ", job.queue_name)
    assert result.exit_code == 0
    assert job.queue_name == 'default'


def test_oarsub_sleep_not_enough_resources_1(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '-l resource_id=10', '"sleep 1"'])
    print(result.output)
    assert result.exit_code == -5


def test_oarsub_sleep_property_error(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '-l resource_id=4', '-p yopyop SELECT', '"sleep 1"'])
    print(result.output)
    assert result.exit_code == -5


def test_oarsub_sleep_queue_error(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q queue_doesnot_exist', '"sleep 1"'])
    print(result.output)
    assert result.exit_code == -8
