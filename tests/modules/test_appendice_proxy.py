# coding: utf-8
from __future__ import unicode_literals, print_function
from oar.modules.appendice_proxy import AppendiceProxy
from oar.lib import config

import pytest

import zmq


sent_msgs = {}
recv_msgs = {}

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
        global sent_msgs
        sent_msgs[num] = []

    def bind(self, url):
        pass

    def connect(self, url):
        pass

    def send(self, msg):
        print("send",self.num, msg)
        global sent_msgs
        sent_msgs[self.num].append(msg)

    def recv_multipart(self):
        print('recv_multipart:', self.num, recv_msgs)
        msg = FakeZmqSocketMessage(recv_msgs[self.num].pop())
        client_id = 1
        return(client_id, msg)


class FakeZmq(object):
    i = 0
    def __init__(self):
        print('FakeZmq')
        pass

    def socket(self, socket_type):
        s = FakeZmqSocket(FakeZmq.i)
        FakeZmq.i += 1
        return(s)


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, 'Context', FakeZmq)


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config['APPENDICE_PROXY_SERVER_PORT'] = '6668'
    @request.addfinalizer
    def teardown():
        del config['APPENDICE_PROXY_SERVER_PORT']

def test_appendice_proxy_simple(monkeypatch):

    print("test.......................")
    #import pdb; pdb.set_trace()
    global recv_msgs
    recv_msgs[0] = ['yop']
    print(recv_msgs, sent_msgs)
    appendice_proxy =  AppendiceProxy()
    print(recv_msgs, sent_msgs)
    appendice_proxy.run(False)

    print(recv_msgs, sent_msgs)
    
    assert sent_msgs[1][0].decode('utf-8') == 'yop'
