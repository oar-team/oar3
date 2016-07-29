# coding: utf-8
from __future__ import unicode_literals, print_function

from oar.modules.almighty import Almighty
from oar.lib import config
from .fakezmq import FakeZmq

import pytest
import zmq

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, 'Context', FakeZmq)

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

def test_almighty(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'FOO'}]
    almighty = Almighty()
    almighty.run(False)
    assert almighty.state == 'Qget'

