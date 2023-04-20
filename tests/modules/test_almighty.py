# coding: utf-8

import signal

import pytest
import zmq

import oar.lib.tools
from oar.lib.globals import init_oar

# from oar.lib import config
from oar.modules.almighty import Almighty, signal_handler

from ..faketools import FakePopen, fake_call, fake_get_date, fake_popen, set_fake_date
from ..fakezmq import FakeZmq

config, db, logger = init_oar()

fakezmq = FakeZmq()

KAO = "/usr/local/lib/oar/kao"
FINAUD = "/usr/local/lib/oar/oar-finaud"
SARKO = "/usr/local/lib/oar/oar-sarko"
LEON = "/usr/local/lib/oar/oar-leon"
NODE_CHANGE_STATE = "/usr/local/lib/oar/oar-node-change-state"


@pytest.fixture(scope="module", autouse=True)
def preserve_signal_handlers():
    yield
    signal.signal(signal.SIGUSR1, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, "Context", FakeZmq)
    monkeypatch.setattr(oar.lib.tools, "get_time", fake_get_date)
    monkeypatch.setattr(oar.lib.tools, "call", fake_call)
    monkeypatch.setattr(oar.lib.tools, "Popen", FakePopen)


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


@pytest.mark.parametrize(
    "command, state",
    [
        ("FOO", "Qget"),
        ("Qsub", "Scheduler"),
        ("Term", "Scheduler"),
        ("BipBip", "Scheduler"),
        ("Scheduling", "Scheduler"),
        ("Qresume", "Scheduler"),
        ("Qdel", "Leon"),
        ("Villains", "Check for villains"),
        ("Finaud", "Check node states"),
        ("Time", "Time update"),
        ("ChState", "Change node state"),
        ("Time update", "Qget"),
    ],
)
def test_almighty_state_Qget(command, state, monkeypatch):
    set_fake_date(1000)
    fakezmq.recv_msgs[0] = [{"cmd": command}]
    almighty = Almighty()
    almighty.run(False)
    assert almighty.state == state
    set_fake_date(0)


@pytest.mark.parametrize(
    "state_in, state_out, called_cmd",
    [
        ("Scheduler", "Time update", KAO),  # Scheduler_1
        ("Check for villains", "Time update", SARKO),
        ("Check node states", "Time update", FINAUD),
        ("Leon", "Time update", LEON),  # Leon 1
        ("Change node state", "Time update", NODE_CHANGE_STATE),
    ],
)
def test_almighty_state_called_command(state_in, state_out, called_cmd, monkeypatch):
    set_fake_date(1000)
    almighty = Almighty()
    almighty.state = state_in
    almighty.run(False)
    assert fake_popen["cmd"] == called_cmd
    assert almighty.state == state_out
    set_fake_date(0)


@pytest.mark.parametrize(
    "state_in, exit_value, state_out, called_cmd",
    [
        ("Scheduler", 2, "Leon", NODE_CHANGE_STATE),
        ("Scheduler", 1, "Scheduler", NODE_CHANGE_STATE),
        ("Scheduler", 0, "Time update", KAO),
        ("Scheduler", [1, 0], "Scheduler", KAO),
        ("Scheduler", [2, 0], "Leon", KAO),
    ],
)
def test_almighty_state_called_command_with_exit_value(
    state_in, exit_value, state_out, called_cmd, monkeypatch
):
    set_fake_date(1000)
    # global fake_called_command
    fake_popen["wait_return_code"] = exit_value
    almighty = Almighty()
    almighty.state = "Scheduler"
    almighty.run(False)
    assert fake_popen["cmd"] == called_cmd
    assert almighty.state == state_out
    fake_popen["wait_return_code"] = 0
    fake_popen["cmd"] = None
    set_fake_date(0)


def test_almighty_no_dup_command():
    almighty = Almighty()
    almighty.command_queue = ["foo_cmd"]
    almighty.add_command("foo_cmd")
    assert almighty.command_queue == ["foo_cmd"]


def test_almighty_Scheduler_scheduler_wanted():
    set_fake_date(0)
    almighty = Almighty()
    almighty.state = "Scheduler"
    # import pdb; pdb.set_trace()
    almighty.run(False)
    assert almighty.scheduler_wanted == 1
    assert almighty.state == "Time update"


def test_almighty_time_update0():
    almighty = Almighty()
    set_fake_date(0)
    almighty.time_update()
    print(str(almighty.command_queue))
    assert almighty.command_queue == []


def test_almighty_time_update():
    almighty = Almighty()
    set_fake_date(1000)
    almighty.time_update()
    print(str(almighty.command_queue))
    assert almighty.command_queue == ["Scheduling", "Villains", "Finaud"]
    set_fake_date(0)


def test_almighty_finishTag():
    # TODO Side effect finishTag == True after this test
    signal_handler(15, [])
    almighty = Almighty()
    exit_code = almighty.run(False)
    assert exit_code == 10
    # This below doesn't work
    # global finishTag
    # finishTag = False
