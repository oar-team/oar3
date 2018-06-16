# coding: utf-8
import pytest
import re

from click.testing import CliRunner

from oar.lib import db
from oar.cli.oarproperty import cli

#@pytest.yield_fixture(scope='function', autouse=True)
#def minimal_db_initialization(request):
#    with db.session(ephemeral=True):
#        yield

def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ['-V'])
    print(result.output)
    assert re.match(r'.*\d\.\d\.\d.*', result.output)

    
def test_oarpropery_simple_error():
    runner = CliRunner()
    
    result = runner.invoke(cli, ['-a core', '-c'])
    print(result.output)
    assert result.exit_code == 2
    
def test_oarpropery_add():
    runner = CliRunner()
    result = runner.invoke(cli, ['-a fancy', '-c'])
    print(result.output)  
    assert result.exit_code == 0

def test_oarpropery_list():
    runner = CliRunner()
    result = runner.invoke(cli, ['--list'])
    print(result.output)
    assert result.output.split('\n')[0] == 'core'
    assert result.exit_code == 0
