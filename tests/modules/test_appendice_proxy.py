# coding: utf-8
from __future__ import unicode_literals, print_function
from oar.modules.appendice_proxy import AppendiceProxy
from oar.lib import config

import pytest

import zmq

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


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config['APPENDICE_PROXY_SERVER_PORT'] = '6668'
    config['BIPBIP_COMMANDER_SERVER'] = 'localhost'
    config['BIPBIP_COMMANDER_PORT'] = '6669'
    @request.addfinalizer
    def teardown():
        del config['APPENDICE_PROXY_SERVER_PORT']
        del config['BIPBIP_COMMANDER_SERVER']
        del config['BIPBIP_COMMANDER_PORT']
        FakeZmq.i = 0
        FakeZmq.sent_msgs = {}
        FakeZmq.recv_msgs = {}
def test_appendice_proxy_simple(monkeypatch):

    FakeZmq.recv_msgs[0] = ['yop']

    appendice_proxy =  AppendiceProxy()
    appendice_proxy.run(False)
    
    assert FakeZmq.sent_msgs[1][0] == {'msg': 'yop'}

def test_appendice_proxy_OAREXEC(monkeypatch):

    FakeZmq.recv_msgs[0] = ['OAREXEC_10_2_N_34']
    
    appendice_proxy = AppendiceProxy()
    appendice_proxy.run(False)
    
    assert FakeZmq.sent_msgs[2][0] == {'job_id': 10, 'args': ['2', 'N', '34'], 'cmd': 'OAREXEC'}

def test_appendice_proxy_OARRUNJOB(monkeypatch):

    FakeZmq.recv_msgs[0] = ['OARRUNJOB_42']
    
    appendice_proxy = AppendiceProxy()
    appendice_proxy.run(False)
    
    assert FakeZmq.sent_msgs[2][0] == {'job_id': 42, 'args': [], 'cmd': 'OARRUNJOB'}

def test_appendice_proxy_LEONEXTERMINATE(monkeypatch):

    FakeZmq.recv_msgs[0] = ['LEONEXTERMINATE_42']
    
    appendice_proxy = AppendiceProxy()
    appendice_proxy.run(False)
    
    assert FakeZmq.sent_msgs[2][0] == {'job_id': 42, 'args': [], 'cmd': 'LEONEXTERMINATE'}
