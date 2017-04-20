# coding: utf-8
from __future__ import unicode_literals, print_function
import oar.lib.tools
from oar.modules.bipbip_commander import BipbipCommander
from oar.lib import config
from .fakezmq import FakeZmq

import time
import pytest

import zmq
import oar.lib.tools

called_command = None

def fake_call(cmd):
    global called_cmd
    called_command = cmd
    
@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, 'Context', FakeZmq)
    monkeypatch.setattr(oar.lib.tools, 'call', fake_call) # TO DEBUG, doesn't work

@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config['SERVER_HOSTNAME'] = 'localhost'
    config['APPENDICE_SERVER_PORT'] = '6668'
    config['BIPBIP_COMMANDER_SERVER'] = 'localhost'
    config['BIPBIP_COMMANDER_PORT'] = '6669'

    @request.addfinalizer
    def teardown():
        del config['SERVER_HOSTNAME']
        del config['APPENDICE_SERVER_PORT']  
        del config['BIPBIP_COMMANDER_SERVER']
        del config['BIPBIP_COMMANDER_PORT']
        FakeZmq.num_socket = 0
        FakeZmq.sent_msgs = {}
        FakeZmq.recv_msgs = {}

def test_bipbip_commander_OAREXEC(monkeypatch):
    FakeZmq.recv_msgs[1] =[{'job_id': 10, 'args': ['2', 'N', '34'], 'cmd': 'OAREXEC'}]
    bipbip_commander = BipbipCommander()
    bipbip_commander.run(False)
    bipbip_commander.bipbip_leon_executors[10].join()
    exitcode = bipbip_commander.bipbip_leon_executors[10].exitcode 
    # TODEBUG assert ['/usr/local/lib/oar/bipbip', '10', '2', 'N', '34'] == called_command
    assert exitcode == 0

