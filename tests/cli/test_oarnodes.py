# coding: utf-8
import re
import pytest

from click.testing import CliRunner

from oar.lib import db
from oar.cli.oarnodes import cli

NB_NODES=5
NB_LINES_PER_NODE=3 # network_address: localhost\n resource_id: 1\n state: Alive\n
@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(NB_NODES):
            db['Resource'].create(network_address="localhost")
        yield

def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ['-V'])
    print(result.output)
    assert re.match(r'.*\d\.\d\.\d.*', result.output)

def test_oarnodes_simple():
    runner = CliRunner()
    result = runner.invoke(cli)
    nb_lines = len(result.output.split('\n'))
    assert nb_lines == NB_LINES_PER_NODE * NB_NODES + 1 # + 1 for last \n
    assert result.exit_code == 0

def test_oarnodes_sql():
    for _ in range(2):
        db['Resource'].create(network_address='akira')
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli,  ['--sql', "network_address=\'akira\'"])
    nb_lines = len(result.output.split('\n'))
    assert nb_lines == NB_LINES_PER_NODE * 2 + 1 # + 1 for last \n
    assert result.exit_code == 0
