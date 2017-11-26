# coding: utf-8

import oar.lib.tools
from oar.modules.hulot import (Hulot, HulotClient, fill_timeouts, get_timeout)
from oar.lib import config
from .fakezmq import FakeZmq

import time
import pytest

import zmq
import oar.lib.tools


# Set undefined config value to default one
DEFAULT_CONFIG = {
    'HULOT_SERVER': 'localhost',
    'HULOT_PORT' : 6670,
    'ENERGY_SAVING_WINDOW_SIZE': 25,
    'ENERGY_SAVING_WINDOW_TIME': 60,
    'ENERGY_SAVING_WINDOW_TIMEOUT': 120,
    'ENERGY_SAVING_NODE_MANAGER_WAKEUP_TIMEOUT': 900,
    'ENERGY_MAX_CYCLES_UNTIL_REFRESH': 5000,
    'OAR_RUNTIME_DIRECTORY': '/var/lib/oar',
    'ENERGY_SAVING_NODES_KEEPALIVE': "type='default':0",
    'ENERGY_SAVING_WINDOW_FORKER_BYPASS': 'yes',
    'ENERGY_SAVING_WINDOW_FORKER_SIZE': 20
}

def fake_call(cmd):
    global called_cmd
    called_command = cmd

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, 'Context', FakeZmq)
    #monkeypatch.setattr(oar.lib.tools, 'call', fake_call) # TO DEBUG, doesn't work


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config.setdefault_config(DEFAULT_CONFIG)
    FakeZmq.reset()

    @request.addfinalizer
    def teardown():
        pass

def test_fill_timeouts_1():
    timeouts = fill_timeouts("10")
    assert timeouts == {1: 10}

def test_fill_timeouts_2():
    timeouts = fill_timeouts("  1:500  11:1000 21:2000 ")
    assert timeouts == {1: 500, 11:1000, 21:2000}

def test_get_timeout(): 
    timeout = get_timeout({1: 500, 11:1000, 21:2000, 30:3000}, 15)
    assert timeout == 1000

def test_hulot_check_1(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'CHECK'}]
    hulot = Hulot()
    hulot.run(False)

def test_hulot_halt_1(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'HALT', 'nodes': ['node1']}]
    hulot = Hulot()
    hulot.run(False)
    # TODO TOFINISH

def test_hulot_wakup_1(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'WAKEUP', 'nodes': ['node1']}]
    hulot = Hulot()
    hulot.run(False)
    # TODO TOFINISH
