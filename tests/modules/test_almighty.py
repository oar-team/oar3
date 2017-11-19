# coding: utf-8

from oar.modules.almighty import Almighty
from oar.lib import config
from .fakezmq import FakeZmq
import oar.lib.tools

import pytest
import zmq

called_command = ''

KAO = '/usr/local/lib/oar/kao'
FINAUD = '/usr/local/lib/oar/finaud'
SARKO = '/usr/local/lib/oar/sarko'
LEON = '/usr/local/lib/oar/Leon'
NODE_CHANGE_STATE = '/usr/local/lib/oar/NodeChangeState'

def fake_call(cmd):
    global called_command
    called_command = cmd
    return 0

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, 'Context', FakeZmq)
    monkeypatch.setattr(oar.lib.tools, 'call', fake_call)
    monkeypatch.setattr(oar.lib.tools, 'Popen', lambda x: None)

@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config['SERVER_HOSTNAME'] = 'localhost'
    config['APPENDICE_SERVER_PORT'] = '6668'
    config['BIPBIP_COMMANDER_SERVER'] = 'localhost'
    config['BIPBIP_COMMANDER_PORT'] = '6669'
    FakeZmq.reset()

    @request.addfinalizer
    def teardown():
        del config['SERVER_HOSTNAME']
        del config['APPENDICE_SERVER_PORT']  
        del config['BIPBIP_COMMANDER_SERVER']
        del config['BIPBIP_COMMANDER_PORT']

@pytest.mark.parametrize("command, state", [
    ('FOO', 'Qget'),
    ('Qsub', 'Scheduler'),
    ('Term', 'Scheduler'),
    ('BipBip', 'Scheduler'),
    ('Scheduling', 'Scheduler'),
    ('Qresume', 'Scheduler'),
    ('Qdel', 'Leon'),
    ('Villains', 'Check for villains'),
    ('Finaud', 'Check node states'),
    ('Time', 'Time update'),
    ('ChState', 'Change node state'),
    ('Time update', 'Qget')
    ])
def test_almighty_state_Qget(command, state, monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': command}]
    almighty = Almighty()
    almighty.run(False)
    assert almighty.state == state


@pytest.mark.parametrize("state_in, state_out, called_cmd", [
    ('Scheduler', 'Time update', KAO), # Scheduler_1
    ('Check for villains', 'Time update', SARKO),
    ('Check node states', 'Time update', FINAUD),
    ('Leon', 'Time update', LEON), # Leon 1
    ('Change node state', 'Time update', NODE_CHANGE_STATE)
])
def test_almighty_state_called_command(state_in, state_out, called_cmd, monkeypatch):
    almighty = Almighty()
    almighty.state = state_in
    almighty.run(False)
    assert called_command == called_cmd
    assert almighty.state == state_out

