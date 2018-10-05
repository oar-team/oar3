# coding: utf-8

import oar.lib.tools
from oar.modules.bipbip_commander import BipbipCommander
from oar.lib import config
from ..fakezmq import FakeZmq
from ..faketools import (fake_call, fake_called_command, FakeProcess, fake_process)

import time
import pytest

import zmq
import oar.lib.tools

fakezmq = FakeZmq()

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, 'Context', FakeZmq)
    monkeypatch.setattr(oar.lib.tools, 'call', fake_call) # TO DEBUG, doesn't work
    monkeypatch.setattr(oar.lib.tools, 'Process', FakeProcess)
    
@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config['SERVER_HOSTNAME'] = 'localhost'
    config['APPENDICE_SERVER_PORT'] = '6670'
    config['BIPBIP_COMMANDER_SERVER'] = 'localhost'
    config['BIPBIP_COMMANDER_PORT'] = '6671'
    fakezmq.reset()

    @request.addfinalizer
    def teardown():
        del config['SERVER_HOSTNAME']
        del config['APPENDICE_SERVER_PORT']
        del config['BIPBIP_COMMANDER_SERVER']
        del config['BIPBIP_COMMANDER_PORT']

def test_bipbip_commander_OAREXEC():
    fakezmq.recv_msgs[0] = [{'job_id': 10, 'args': ['2', 'N', '34'], 'cmd': 'OAREXEC'}]
    bipbip_commander = BipbipCommander()
    bipbip_commander.run(False)
    #bipbip_commander.bipbip_leon_executors[10].join()
    #exitcode = bipbip_commander.bipbip_leon_executors[10].exitcode
    print(fake_called_command)
    assert bipbip_commander.bipbip_leon_commands_to_run == []
    assert ['/usr/local/lib/oar/oar3-bipbip', '10', '2', 'N', '34'] == fake_called_command['cmd']

def test_bipbip_commander_LEONEXTERMINATE():
    fakezmq.recv_msgs[0] = [{'job_id': 10, 'cmd': 'LEONEXTERMINATE'}]
    bipbip_commander = BipbipCommander()
    bipbip_commander.run(False)
    #bipbip_commander.bipbip_leon_executors[10].join()
    #exitcode = bipbip_commander.bipbip_leon_executors[10].exitcode
    print(fake_called_command)
    print(bipbip_commander.bipbip_leon_commands_to_run)
    assert bipbip_commander.bipbip_leon_commands_to_run == []
    assert ['/usr/local/lib/oar/oar3-leon', '10'] == fake_called_command['cmd']
    
def test_bipbip_commander_LEONEXTERMINATE2():
    fakezmq.recv_msgs[0] = [{'job_id': 10, 'cmd': 'LEONEXTERMINATE'},
                            {'job_id': 10, 'cmd': 'LEONEXTERMINATE'}]
    bipbip_commander = BipbipCommander()
    
    bipbip_commander.run(False)
    #import pdb; pdb.set_trace()
    bipbip_commander.run(False)
    
    #bipbip_commander.bipbip_leon_executors[10].join()
    #exitcode = bipbip_commander.bipbip_leon_executors[10].exitcode
    print(fake_called_command)
    print(bipbip_commander.bipbip_leon_commands_to_run)
    assert bipbip_commander.bipbip_leon_commands_to_run[0]['job_id'] == 10
    assert ['/usr/local/lib/oar/oar3-leon', '10'] == fake_called_command['cmd']
