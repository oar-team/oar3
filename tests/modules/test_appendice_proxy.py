# coding: utf-8

import pytest
import zmq

# from oar.lib import config
from oar.modules.appendice_proxy import AppendiceProxy
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from ..fakezmq import FakeZmq


config, db, logger = init_oar()

fakezmq = FakeZmq()


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, "Context", FakeZmq)


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config["SERVER_HOSTNAME"] = "localhost"
    config["APPENDICE_SERVER_PORT"] = "6670"
    config["BIPBIP_COMMANDER_SERVER"] = "localhost"
    config["BIPBIP_COMMANDER_PORT"] = "6671"
    fakezmq.reset()

    @request.addfinalizer
    def teardown():
        del config["SERVER_HOSTNAME"]
        del config["APPENDICE_SERVER_PORT"]
        del config["BIPBIP_COMMANDER_SERVER"]
        del config["BIPBIP_COMMANDER_PORT"]


def test_appendice_proxy_simple(monkeypatch):
    fakezmq.recv_msgs[0] = ["yop"]

    appendice_proxy = AppendiceProxy()
    appendice_proxy.run(False)

    assert fakezmq.sent_msgs[1][0] == {"cmd": "yop"}


def test_appendice_proxy_OAREXEC(monkeypatch):
    fakezmq.recv_msgs[0] = ["OAREXEC_10_2_N_34"]

    appendice_proxy = AppendiceProxy()
    appendice_proxy.run(False)

    assert fakezmq.sent_msgs[2][0] == {
        "job_id": 10,
        "args": ["2", "N", "34"],
        "cmd": "OAREXEC",
    }


def test_appendice_proxy_OARRUNJOB(monkeypatch):
    fakezmq.recv_msgs[0] = ["OARRUNJOB_42"]

    appendice_proxy = AppendiceProxy()
    appendice_proxy.run(False)

    assert fakezmq.sent_msgs[2][0] == {"job_id": 42, "args": [], "cmd": "OARRUNJOB"}


def test_appendice_proxy_LEONEXTERMINATE(monkeypatch):
    fakezmq.recv_msgs[0] = ["LEONEXTERMINATE_42"]

    appendice_proxy = AppendiceProxy()
    appendice_proxy.run(False)

    assert fakezmq.sent_msgs[2][0] == {
        "job_id": 42,
        "args": [],
        "cmd": "LEONEXTERMINATE",
    }
