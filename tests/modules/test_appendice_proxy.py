# coding: utf-8
from __future__ import unicode_literals, print_function
from oar.modules.appendice_proxy import AppendiceProxy
from oar.lib import config
from .fakezmq import FakeZmq

import pytest
import zmq

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
    
    assert FakeZmq.sent_msgs[1][0] == {'cmd': 'yop'}

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
