# coding: utf-8
import pytest
import os
import re
from click.testing import CliRunner

from oar.lib import (db, config)
from oar.lib.job_handling import get_job_types
from oar.cli.oarsub import cli

import oar.lib.tools  # for monkeypatching

default_res = '/resource_id=1'
nodes_res = 'resource_id'

fake_popen_process_stdout = ''
class FakeProcessStdout(object):
    def __init__(self):
        pass
    def decode(self):
        return fake_popen_process_stdout
 
class FakePopen(object):
    def __init__(self, cmd, stdout):
        pass
    def communicate(self):
        process_sdtout = FakeProcessStdout() 
        return [process_sdtout]

@pytest.fixture(scope="module", autouse=True)
def set_env(request):
    os.environ['OARDIR'] = '/tmp'
    os.environ['OARDO_USER'] = 'yop'


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'Popen', FakePopen)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: True)

def test_oarsub_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exception.code == \
        (5, 'Command or interactive flag or advance reservation time or connection directive must be provided')

def test_oarsub_version():
    runner = CliRunner()
    result = runner.invoke(cli, ['-V'])
    print(result.output)
    assert re.match(r'.*\d\.\d\.\d.*', result.output)
    
def test_oarsub_sleep_1(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '"sleep 1"'])
    print(result.output)
    # job = db['Job'].query.one()
    mld_job_desc = db['MoldableJobDescription'].query.one()
    job_res_desc = db['JobResourceDescription'].query.one()
    print(mld_job_desc.walltime, job_res_desc.resource_type, job_res_desc.value)
    assert result.exit_code == 0
    assert mld_job_desc.walltime == config['DEFAULT_JOB_WALLTIME']
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
    assert mld_job_desc.walltime == config['DEFAULT_JOB_WALLTIME']
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

def test_oarsub_parameters(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '--project', 'batcave',
                                 '--name', 'yop',
                                 '--notify', "mail:name\@domain.com", '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    print("project: ", job.project)
    assert result.exit_code == 0
    assert job.project == 'batcave'
    assert job.name == 'yop'
    assert job.notify == 'mail:name\@domain.com'

def test_oarsub_directory(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '-d', '/home/robin/batcave', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    print("directory: ", job.launching_directory)
    assert result.exit_code == 0
    assert job.launching_directory == '/home/robin/batcave'
    
def test_oarsub_stdout_stderr(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '-O', 'foo_%jobid%o', '-E', 'foo_%jobid%e', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    assert result.exit_code == 0
    assert job.stdout_file == 'foo_%jobid%o'
    assert job.stderr_file == 'foo_%jobid%e'

def test_oarsub_admission_queue_1(monkeypatch):

    db['AdmissionRule'].create(rule=("if user == 'yop':"
                                     "    queue= 'default'"))

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
    assert result.exception.code == (-5, 'There are not enough resources for your request')


def test_oarsub_sleep_property_error(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '-l resource_id=4', '-p yopyop SELECT', '"sleep 1"'])
    print(result.output)
    assert result.exception.code[0] == -5


def test_oarsub_sleep_queue_error(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q queue_doesnot_exist', '"sleep 1"'])
    print(result.output)
    assert result.exception.code == (-8, 'queue queue_doesnot_exist does not exist')

def test_oarsub_interactive_reservation_error():
    runner = CliRunner()
    result = runner.invoke(cli, ['-I', '-r', '2018-02-06 14:48:0'])
    print(result.output)
    assert result.exception.code == (7, 'An advance reservation cannot be interactive.')

def test_oarsub_interactive_desktop_computing_error():
    runner = CliRunner()
    result = runner.invoke(cli, ['-I', '-t desktop_computing'])
    print(result.output)
    assert result.exception.code == (17, 'A desktop computing job cannot be interactive')

def test_oarsub_interactive_noop_error():
    runner = CliRunner()
    result = runner.invoke(cli, ['-I', '-t noop'])
    print(result.output)
    assert result.exception.code == (17, 'a NOOP job cannot be interactive.')

def test_oarsub_connect_noop_error():
    runner = CliRunner()
    result = runner.invoke(cli, ['-C 1234', '-t noop'])
    print(result.output)
    assert result.exception.code == (17, 'A NOOP job does not have a shell to connect to.')  

def test_oarsub_scanscript_1():
    global fake_popen_process_stdout
    fake_popen_process_stdout = ("#Funky job\n"
                                 "#OAR -l resource_id=4,walltime=1\n"
                                 "#OAR -n funky\n"
                                 "#OAR --project batcave\n")

    runner = CliRunner()
    result = runner.invoke(cli, ['-S', 'yop'])
    print(result.output)
    job = db['Job'].query.one()
    print(job.initial_request)
    assert job.name == 'funky'
    assert job.project == 'batcave'

def test_oarsub_multiple_types(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q', 'default', '-t', 't1', '-t', 't2', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    job_types = get_job_types(job.id)
    print(job_types)
    assert job_types == {'t1': True, 't2': True}
    assert result.exit_code == 0

    
