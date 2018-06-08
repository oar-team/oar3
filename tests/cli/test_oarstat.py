# coding: utf-8
import pytest
import re
from ..helpers import insert_terminated_jobs

from click.testing import CliRunner

from oar.lib import db,Job
from oar.cli.oarstat import cli
from oar.lib.job_handling import insert_job
from oar.lib.event import add_new_event
import oar.lib.tools  # for monkeypatching

from oar.lib.utils import print_query_results

NB_JOBS=5


@pytest.yield_fixture(scope='function')
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(10):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield

@pytest.fixture(scope='function')
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'get_username', lambda: 'zozo')


def test_oarstat_simple():
    for _ in range(NB_JOBS):
        insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli)
    nb_lines = len(result.output_bytes.decode().split('\n'))
    print(result.output_bytes.decode())
    assert nb_lines == NB_JOBS + 3
    assert result.exit_code == 0


def test_oarstat_sql_property():
    for i in range(NB_JOBS):
        insert_job(res=[(60, [('resource_id=4', "")])], properties='', user=str(i))
    runner = CliRunner()
    result = runner.invoke(cli,  ['--sql', "(job_user=\'2\' OR job_user=\'3\')"])
    print(result.output_bytes.decode())
    nb_lines = len(result.output_bytes.decode().split('\n'))
    assert nb_lines == 5
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') != 'postgresql'",
                    reason="need postgresql database")
def xtest_oarstat_accounting(minimal_db_initialization):
    insert_terminated_jobs()
    runner = CliRunner()
    result = runner.invoke(cli, ['--accounting', '1970-01-01, 1970-01-20'])
    str_result = result.output_bytes.decode()
    print(str_result)
    print(str_result.split('\n'))
    assert re.match(r'.*8640000.*', str_result.split('\n')[2])

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') != 'postgresql'",
                    reason="need postgresql database")
def test_oarstat_accounting_user(minimal_db_initialization, monkeypatch_tools):
    insert_terminated_jobs()
    karma = ' Karma=0.345'
    insert_job(res=[(60, [('resource_id=2', '')])],
               properties='', command='yop', user='zozo', project='yopa',
               start_time=0, message=karma)
    runner = CliRunner()
    result = runner.invoke(cli, ['--accounting', '1970-01-01, 1970-01-20', '--user'])
    str_result = result.output_bytes.decode()
    print(str_result)
    print(str_result.split('\n')[-2])
    assert re.match(r'.*Karma.*0.345.*', str_result.split('\n')[-2])

def test_oarstat_gantt(minimal_db_initialization):
    insert_terminated_jobs(update_accounting=False)

    jobs = db.query(Job).all()
    print_query_results(jobs)
            
    for j in jobs:
        print(j.id, j.assigned_moldable_job)
    #import pdb; pdb.set_trace()
    runner = CliRunner()
    result = runner.invoke(cli, ['--gantt', '1970-01-01 01:20:00, 1970-01-20 00:00:00'])
    str_result = result.output_bytes.decode()
    print(str_result)
    assert re.match('.*10 days.*', str_result.split('\n')[3])

def test_oarstat_events(minimal_db_initialization):

    #j_ids = []
    #for _ in range(5):
    #    j_ids.append(insert_job(res=[(60, [('resource_id=4', "")])]))

    job_id = insert_job(res=[(60, [('resource_id=4', "")])])
    add_new_event('EXECUTE_JOB', job_id, 'Have a good day !')
    
    runner = CliRunner()
    result = runner.invoke(cli, ['--events', '--job', str(job_id)])
    
    str_result = result.output_bytes.decode()
    print(str_result)
    assert re.match('.*EXECUTE_JOB.*', str_result)
