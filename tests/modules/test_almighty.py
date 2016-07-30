# coding: utf-8
from __future__ import unicode_literals, print_function

from oar.modules.almighty import Almighty
from oar.lib import config
from .fakezmq import FakeZmq
import oar.lib.tools

import pytest
import zmq

called_command = ''

def fake_call(cmd):
    global called_command
    called_command = cmd
    return 0

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, 'Context', FakeZmq)
    monkeypatch.setattr(oar.lib.tools, 'call', fake_call)

@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config['ZMQ_SERVER_PORT'] = '6667'
    config['APPENDICE_PROXY_SERVER_PORT'] = '6668'
    
    config['BIPBIP_COMMANDER_SERVER'] = 'localhost'
    config['BIPBIP_COMMANDER_PORT'] = '6669'

    @request.addfinalizer
    def teardown():
        del config['ZMQ_SERVER_PORT']
        del config['BIPBIP_COMMANDER_SERVER']
        del config['BIPBIP_COMMANDER_PORT']
        FakeZmq.i = 0
        FakeZmq.sent_msgs = {}
        FakeZmq.recv_msgs = {}

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
    ('ChState', 'Change node state')
    ])
def test_almighty_state_Qget(command, state, monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': command}]
    almighty = Almighty()
    almighty.run(False)
    assert almighty.state == state

def test_almighty_state_Scheduler_1(monkeypatch):
    # TODO
    almighty = Almighty()
    almighty.state = 'Scheduler'
    print(called_command)
    almighty.run(False)

    assert called_command == '/usr/local/lib/oar/kao'
    assert almighty.state == 'Time update'
    
def test_almighty_state_Time_update(monkeypatch):
    #FakeZmq.recv_msgs[0] = [{'cmd': command}]
    almighty = Almighty()
    almighty.state = 'Time update'
    almighty.run(False)
    assert almighty.state == 'Qget'
    
def test_almighty_state_Check_for_villains_1(monkeypatch):

    almighty = Almighty()
    almighty.state = 'Check for villains'
    almighty.run(False)
    assert called_command == '/usr/local/lib/oar/sarko'
    assert almighty.state == 'Time update'
    
# CHECK FOR VILLAINS
# CHECK NODE STATES
# LEON
# Change state for dynamic nodes
