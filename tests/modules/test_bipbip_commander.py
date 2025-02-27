# coding: utf-8

import pytest
import zmq

import oar.lib.tools
from oar.modules.bipbip_commander import BipbipCommander

from ..faketools import (
    FakePopen,
    FakeProcess,
    fake_call,
    fake_called_command,
    fake_popen,
)
from ..fakezmq import FakeZmq

fakezmq = FakeZmq()


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, "Context", FakeZmq)
    monkeypatch.setattr(oar.lib.tools, "call", fake_call)  # TO DEBUG, doesn't work
    monkeypatch.setattr(oar.lib.tools, "Popen", FakePopen)
    monkeypatch.setattr(oar.lib.tools, "Process", FakeProcess)


@pytest.fixture(scope="function", autouse=True)
def setup(request, setup_config):
    setup_config, engine = setup_config
    setup_config["SERVER_HOSTNAME"] = "localhost"
    setup_config["APPENDICE_SERVER_PORT"] = "6670"
    setup_config["BIPBIP_COMMANDER_SERVER"] = "localhost"
    setup_config["BIPBIP_COMMANDER_PORT"] = "6671"

    fakezmq.reset()

    yield

    # del setup_config["SERVER_HOSTNAME"]
    # del setup_config["APPENDICE_SERVER_PORT"]
    # del setup_config["BIPBIP_COMMANDER_SERVER"]
    # del setup_config["BIPBIP_COMMANDER_PORT"]


def test_bipbip_commander_OAREXEC(setup):
    config = setup
    fakezmq.recv_msgs[0] = [{"job_id": 10, "args": ["2", "N", "34"], "cmd": "OAREXEC"}]
    bipbip_commander = BipbipCommander(config)
    bipbip_commander.run(False)
    # bipbip_commander.bipbip_leon_executors[10].join()
    # exitcode = bipbip_commander.bipbip_leon_executors[10].exitcode
    print("helloe", fake_popen)
    assert bipbip_commander.bipbip_leon_commands_to_run == []
    assert [
        "/usr/local/lib/oar/oar-bipbip",
        "10",
        "2",
        "N",
        "34",
    ] == fake_popen[
        "cmd"
    ].split(" ")


def test_bipbip_commander_LEONEXTERMINATE(setup_config, setup):
    config = setup
    fakezmq.recv_msgs[0] = [{"job_id": 10, "cmd": "LEONEXTERMINATE"}]
    bipbip_commander = BipbipCommander(config)
    bipbip_commander.run(False)
    # bipbip_commander.bipbip_leon_executors[10].join()
    # exitcode = bipbip_commander.bipbip_leon_executors[10].exitcode
    print(bipbip_commander.bipbip_leon_commands_to_run)
    assert bipbip_commander.bipbip_leon_commands_to_run == []
    assert ["/usr/local/lib/oar/oar-leon", "10"] == fake_popen["cmd"].split(" ")


def test_bipbip_commander_LEONEXTERMINATE2(setup_config):
    config, db = setup_config
    fakezmq.recv_msgs[0] = [
        {"job_id": 10, "cmd": "LEONEXTERMINATE"},
        {"job_id": 10, "cmd": "LEONEXTERMINATE"},
    ]
    bipbip_commander = BipbipCommander(config)

    bipbip_commander.run(False)
    # import pdb; pdb.set_trace()
    bipbip_commander.run(False)

    # bipbip_commander.bipbip_leon_executors[10].join()
    # exitcode = bipbip_commander.bipbip_leon_executors[10].exitcode
    print(fake_called_command)
    print(bipbip_commander.bipbip_leon_commands_to_run)
    assert bipbip_commander.bipbip_leon_commands_to_run[0]["job_id"] == 10
    assert ["/usr/local/lib/oar/oar-leon", "10"] == fake_popen["cmd"].split()
