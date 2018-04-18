# coding: utf-8
import pytest

from click.testing import CliRunner

from oar.lib import db
from oar.cli.oarnodesetting import cli

import oar.lib.tools  # for monkeypatching
fake_notifications = []

NB_NODES=0

def fake_notify_almighty(notification):
    global fake_notifications
    fake_notifications.append(notification)

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(NB_NODES):
            db['Resource'].create(network_address="localhost")
        yield

        
@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', fake_notify_almighty)

def test_oarnodesetting_simple():
    runner = CliRunner()
    result = runner.invoke(cli, ['-a'])
    resource = db['Resource'].query.one()
    print(resource)
    #nb_lines = len(result.output_bytes.decode().split('\n'))
    #assert nb_lines == NB_LINES_PER_NODE * NB_NODES + 1 # + 1 for last \n
    assert resource.state == 'Alive'
    assert fake_notifications[-2:] == ['ChState', 'Term']
    assert result.exit_code == 0

def test_oarnodesetting_core_cpu():
    runner = CliRunner()
    result = runner.invoke(cli, ['-a', '-p core=1', '-p cpu=2'])
    print(result)
    resource = db['Resource'].query.one()
    print(resource)
    print(fake_notifications)
    #import pdb; pdb.set_trace()
    assert resource.core == 1
    assert resource.cpu == 2
    assert fake_notifications[-3:] == ['Term','ChState', 'Term']
    assert result.exit_code == 0


