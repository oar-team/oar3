# coding: utf-8
import pytest
import re
from ..helpers import insert_terminated_jobs

from click.testing import CliRunner

from oar.lib import db
from oar.cli.oarstat import cli
from oar.lib.job_handling import insert_job

NB_JOBS=5


@pytest.yield_fixture(scope='function')
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(10):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield


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
def test_oarstat_accounting(minimal_db_initialization):
    insert_terminated_jobs()
    karma = ' Karma=0.345'
    insert_job(res=[(60, [('resource_id=2', '')])],
               properties='', command='yop', message=karma)
    runner = CliRunner()
    result = runner.invoke(cli, ['--accounting', '1970-01-01, 1970-01-20'])
    str_result = result.output_bytes.decode()
    print(str_result)
    print(str_result.split('\n'))
    assert re.match(r'.*8640000.*', str_result.split('\n')[2])
    
    
