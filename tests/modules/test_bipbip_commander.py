# coding: utf-8
from __future__ import unicode_literals, print_function
import oar.lib.tools
from oar.modules.bipbip_commander import BipbipCommander
from oar.lib import config

import pytest

import zmq
import oar.lib.tools

called_command = []

def fake_call(cmd):
    global called_cmd
    called_command = cmd


class FakeZmqSocketMessage(object):
    def __init__(self, msg):
        print("FakeZmqSocketMessage", msg)
        self.msg = msg

    def decode(self, fmt):
        return(self.msg)

class FakeZmqSocket(object):
    def __init__(self, num):
        print("FakeZmqSocket")
        self.num = num
        FakeZmq.sent_msgs[num] = []

    def bind(self, url):
        pass

    def connect(self, url):
        pass

    def send(self, msg):
        print("send",self.num, msg)
        FakeZmq.sent_msgs[self.num].append(msg)

    def send_json(self, msg):
        print("send_json",self.num, msg)
        FakeZmq.sent_msgs[self.num].append(msg)

    def recv_json(self):
        return FakeZmq.recv_msgs[self.num].pop()
        
    def recv_multipart(self):
        print('recv_multipart:', self.num, FakeZmq.recv_msgs)
        msg = FakeZmqSocketMessage(FakeZmq.recv_msgs[self.num].pop())
        client_id = 1
        return(client_id, msg)


class FakeZmq(object):
    i = 0
    sent_msgs = {}
    recv_msgs = {}
    def __init__(self):
        pass

    def socket(self, socket_type):
        s = FakeZmqSocket(FakeZmq.i)
        FakeZmq.i += 1
        return s

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, 'Context', FakeZmq)
    monkeypatch.setattr(oar.lib.tools, 'call', fake_call) # TO DEBUG, doesn't work

@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config['SERVER_HOSTNAME'] = 'localhost'
    config['ZMQ_SERVER_PORT'] = '6667'
    config['BIPBIP_COMMANDER_SERVER'] = 'localhost'
    config['BIPBIP_COMMANDER_PORT'] = '6669'

    @request.addfinalizer
    def teardown():
        del config['SERVER_HOSTNAME'] 
        del config['ZMQ_SERVER_PORT']
        del config['BIPBIP_COMMANDER_SERVER']
        del config['BIPBIP_COMMANDER_PORT']

def test_bipbip_commander_OAREXEC(monkeypatch):
    FakeZmq.recv_msgs[1] =[{'job_id': 10, 'args': ['2', 'N', '34'], 'cmd': 'OAREXEC'}]
    bipbip_commander = BipbipCommander()
    bipbip_commander.run(False)
    bipbip_commander.bipbip_leon_executors[10].join()
    exitcode = bipbip_commander.bipbip_leon_executors[10].exitcode 
    print(exitcode)
    # TODEBUG assert ['/usr/local/lib/oar/bipbip', '10', '2', 'N', '34'] == called_command
    assert exitcode == 0

