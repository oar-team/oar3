# coding: utf-8
import time

import pytest
import zmq
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools
import oar.lib.tools as tools
from oar.lib.database import ephemeral_session
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from oar.lib.models import Resource
from oar.modules.hulot import (
    Hulot,
    HulotClient,
    WindowForker,
    command_executor,
    fill_timeouts,
    get_timeout,
)

from ..fakezmq import FakeZmq

fakezmq = FakeZmq()

_, _, log = init_oar()

logger = get_logger(log, "test_sarko")


# Set undefined config value to default one
DEFAULT_CONFIG = {
    "HULOT_SERVER": "localhost",
    "HULOT_PORT": 6672,
    "ENERGY_SAVING_WINDOW_SIZE": 25,
    "ENERGY_SAVING_WINDOW_TIME": 60,
    "ENERGY_SAVING_WINDOW_TIMEOUT": 120,
    "ENERGY_SAVING_NODE_MANAGER_WAKEUP_TIMEOUT": 900,
    "ENERGY_MAX_CYCLES_UNTIL_REFRESH": 5000,
    "OAR_RUNTIME_DIRECTORY": "/var/lib/oar",
    "ENERGY_SAVING_NODES_KEEPALIVE": "type='default':0",
    "ENERGY_SAVING_WINDOW_FORKER_BYPASS": "yes",
    "ENERGY_SAVING_WINDOW_FORKER_SIZE": 20,
    "ENERGY_SAVING_NODE_MANAGER_WAKE_UP_CMD": "wakeup_cmd",
    "ENERGY_SAVING_NODE_MANAGER_SLEEP_CMD": "sleep_cmd",
}

called_command = ""


def fake_call(cmd, shell):
    global called_command
    called_command = cmd
    return 0


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, "Context", FakeZmq)
    monkeypatch.setattr(oar.lib.tools, "call", fake_call)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)


@pytest.fixture(scope="function", autouse=True)
def setup(request, setup_config):
    config, _, _ = setup_config
    config.setdefault_config(DEFAULT_CONFIG)
    fakezmq.reset()

    oar.lib.tools.zmq_context = None
    oar.lib.tools.almighty_socket = None
    oar.lib.tools.bipbip_commander_socket = None

    yield config

    @request.addfinalizer
    def teardown():
        global called_command
        called_command = ""


@pytest.fixture(scope="function")
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources

        # available_upto=0 : to disable the wake-up and halt
        # available_upto=1 : to disable the wake-up (but not the halt)
        # available_upto=2147483647 : to disable the halt (but not the wake-up)
        # available_upto=2147483646 : to enable wake-up/halt forever
        # available_upto=<timestamp> : to enable the halt, and the wake-up until the date given by <timestamp>
        for i in range(2):
            Resource.create(
                session, network_address="localhost0", available_upto=2147483646
            )
        for i in range(2):
            Resource.create(
                session, network_address="localhost1", available_upto=2147483646
            )
        for i in range(2):
            Resource.create(
                session,
                network_address="localhost2",
                state="Absent",
                available_upto=2147483646,
            )
        for i in range(2):
            Resource.create(
                session,
                network_address="localhost3",
                state="Absent",
                available_upto=2147483646,
            )
        session.commit()
        yield session


def test_fill_timeouts_1(setup_config):
    timeouts = fill_timeouts("10")
    assert timeouts == {1: 10}


def test_fill_timeouts_2(setup_config):
    timeouts = fill_timeouts("  1:500  11:1000 21:2000 ")
    assert timeouts == {1: 500, 11: 1000, 21: 2000}


def test_get_timeout(setup_config):
    timeout = get_timeout({1: 500, 11: 1000, 21: 2000, 30: 3000}, 15)
    assert timeout == 1000


def test_bad_energy_saving_nodes_keepalive_1(setup_config, setup):
    config = setup
    config["ENERGY_SAVING_NODES_KEEPALIVE"] = "bad"
    hulot = Hulot(config, logger)
    config["ENERGY_SAVING_NODES_KEEPALIVE"] = "type='default':0"
    assert hulot.exit_code == 3


def test_bad_energy_saving_nodes_keepalive_2(setup_config, setup):
    config = setup
    config["ENERGY_SAVING_NODES_KEEPALIVE"] = "type='default':3, bad:bad"
    hulot = Hulot(config, logger)
    config["ENERGY_SAVING_NODES_KEEPALIVE"] = "type='default':0"
    assert hulot.exit_code == 2


def test_hulot_check_simple(
    monkeypatch, setup_config, setup, minimal_db_initialization
):
    config = setup
    fakezmq.recv_msgs[0] = [{"cmd": "CHECK"}]
    print("wut?!", minimal_db_initialization)
    hulot = Hulot(config, logger)
    exit_code = hulot.run(minimal_db_initialization, False)
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_bad_command(monkeypatch, setup_config, minimal_db_initialization, setup):
    config = setup
    fakezmq.recv_msgs[0] = [{"cmd": "BAD_COMMAND", "nodes": ["localhost0"]}]
    hulot = Hulot(config, logger)
    exit_code = hulot.run(minimal_db_initialization, False)
    assert exit_code == 1


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_check_nodes_to_remind(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    fakezmq.recv_msgs[0] = [{"cmd": "CHECK"}]
    hulot = Hulot(config, logger)
    hulot.nodes_list_to_remind = {"localhost0": {"timeout": -1, "command": "HALT"}}
    exit_code = hulot.run(minimal_db_initialization, False)
    print(hulot.nodes_list_running)
    assert "localhost0" in hulot.nodes_list_running
    assert hulot.nodes_list_running["localhost0"]["command"] == "HALT"
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_check_wakeup_for_min_nodes(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    # localhost2 to wakeup
    prev_value = config["ENERGY_SAVING_NODES_KEEPALIVE"]
    config["ENERGY_SAVING_NODES_KEEPALIVE"] = "type='default':3"
    fakezmq.recv_msgs[0] = [{"cmd": "CHECK"}]
    hulot = Hulot(config, logger)
    exit_code = hulot.run(minimal_db_initialization, False)
    config["ENERGY_SAVING_NODES_KEEPALIVE"] = prev_value
    print(hulot.nodes_list_running)
    assert "localhost2" in hulot.nodes_list_running
    assert hulot.nodes_list_running["localhost2"]["command"] == "WAKEUP"
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_halt_1(monkeypatch, setup_config, minimal_db_initialization, setup):
    config = setup
    fakezmq.recv_msgs[0] = [{"cmd": "HALT", "nodes": ["localhost0"]}]
    hulot = Hulot(config, logger)
    exit_code = hulot.run(minimal_db_initialization, False)
    print(hulot.nodes_list_running)
    assert "localhost0" in hulot.nodes_list_running
    assert hulot.nodes_list_running["localhost0"]["command"] == "HALT"
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_halt_keepalive(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    prev_value = config["ENERGY_SAVING_NODES_KEEPALIVE"]
    config["ENERGY_SAVING_NODES_KEEPALIVE"] = "type='default':3"
    fakezmq.recv_msgs[0] = [{"cmd": "HALT", "nodes": ["localhost0"]}]
    hulot = Hulot(config, logger)
    # import pdb; pdb.set_trace()
    exit_code = hulot.run(minimal_db_initialization, False)
    config["ENERGY_SAVING_NODES_KEEPALIVE"] = prev_value
    print(hulot.nodes_list_running)
    assert "localhost2" in hulot.nodes_list_running
    assert hulot.nodes_list_running["localhost2"]["command"] == "WAKEUP"
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_halt_1_forker(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    config["ENERGY_SAVING_WINDOW_FORKER_BYPASS"] = "no"
    fakezmq.recv_msgs[0] = [{"cmd": "HALT", "nodes": ["localhost0"]}]
    hulot = Hulot(config, logger)
    exit_code = hulot.run(minimal_db_initialization, False)
    config["ENERGY_SAVING_WINDOW_FORKER_BYPASS"] = "yes"
    print(hulot.nodes_list_running)
    assert "localhost0" in hulot.nodes_list_running
    assert hulot.nodes_list_running["localhost0"]["command"] == "HALT"
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_wakeup_1(monkeypatch, setup_config, minimal_db_initialization, setup):
    config = setup
    fakezmq.recv_msgs[0] = [{"cmd": "WAKEUP", "nodes": ["localhost2"]}]
    hulot = Hulot(config, logger)
    exit_code = hulot.run(minimal_db_initialization, False)
    print(hulot.nodes_list_running)
    assert "localhost2" in hulot.nodes_list_running
    assert hulot.nodes_list_running["localhost2"]["command"] == "WAKEUP"
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_wakeup_already_timeouted(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    fakezmq.recv_msgs[0] = [{"cmd": "WAKEUP", "nodes": ["localhost2"]}]
    hulot = Hulot(config, logger)
    hulot.nodes_list_running = {"localhost2": {"timeout": -1, "command": "WAKEUP"}}
    exit_code = hulot.run(minimal_db_initialization, False)
    print(hulot.nodes_list_running)
    assert hulot.nodes_list_running == {}
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_wakeup_already_pending(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    fakezmq.recv_msgs[0] = [{"cmd": "WAKEUP", "nodes": ["localhost2"]}]
    hulot = Hulot(config, logger)
    hulot.nodes_list_running = {
        "localhost2": {
            "timeout": tools.get_date(minimal_db_initialization) + 1000,
            "command": "WAKEUP",
        }
    }
    exit_code = hulot.run(minimal_db_initialization, False)
    print(hulot.nodes_list_running)
    print(hulot.nodes_list_to_remind)
    assert "localhost2" in hulot.nodes_list_running
    assert hulot.nodes_list_to_remind == {}
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_halt_wakeup_already_pending(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    fakezmq.recv_msgs[0] = [{"cmd": "HALT", "nodes": ["localhost2"]}]
    hulot = Hulot(config, logger)
    hulot.nodes_list_running = {
        "localhost2": {
            "timeout": tools.get_date(minimal_db_initialization) + 1000,
            "command": "WAKEUP",
        }
    }
    exit_code = hulot.run(minimal_db_initialization, False)
    print(hulot.nodes_list_running)
    print(hulot.nodes_list_to_remind)
    assert "localhost2" in hulot.nodes_list_running
    assert "localhost2" in hulot.nodes_list_to_remind
    assert hulot.nodes_list_to_remind["localhost2"]["command"] == "HALT"
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_check_clean_booted_node(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    fakezmq.recv_msgs[0] = [{"cmd": "CHECK"}]
    hulot = Hulot(config, logger)
    hulot.nodes_list_running = {"localhost0": {"timeout": -1, "command": "WAKEUP"}}
    exit_code = hulot.run(minimal_db_initialization, False)
    print(hulot.nodes_list_running)
    assert hulot.nodes_list_running == {}
    assert exit_code == 0


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_wakeup_1_forker(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    config["ENERGY_SAVING_WINDOW_FORKER_BYPASS"] = "no"
    fakezmq.recv_msgs[0] = [{"cmd": "WAKEUP", "nodes": ["localhost2"]}]
    hulot = Hulot(config, logger)
    exit_code = hulot.run(minimal_db_initialization, False)
    config["ENERGY_SAVING_WINDOW_FORKER_BYPASS"] = "yes"
    print(hulot.nodes_list_running)
    assert "localhost2" in hulot.nodes_list_running
    assert hulot.nodes_list_running["localhost2"]["command"] == "WAKEUP"
    assert exit_code == 0


def test_hulot_client(monkeypatch, setup_config, minimal_db_initialization, setup):
    config = setup
    hulot_ctl = HulotClient(config, logger)
    hulot_ctl.check_nodes()
    assert fakezmq.sent_msgs[0][0] == {"cmd": "CHECK"}
    hulot_ctl.halt_nodes("localhost")
    assert fakezmq.sent_msgs[0][1] == {"cmd": "HALT", "nodes": "localhost"}
    hulot_ctl.wake_up_nodes("localhost")
    assert fakezmq.sent_msgs[0][2] == {"cmd": "WAKEUP", "nodes": "localhost"}


def test_hulot_command_executor(
    monkeypatch, setup_config, minimal_db_initialization, setup
):
    config = setup
    assert command_executor(("HALT", "node1"), config) == 0
    print(called_command)
    assert called_command == 'echo "node1" | sleep_cmd'
    assert command_executor(("WAKEUP", "node1"), config) == 0
    print(called_command)
    assert called_command == 'echo "node1" | wakeup_cmd'


def yop(a, b=0):
    time.sleep(b)
    return a


def test_hulot_window_forker_check_executors(
    setup_config, minimal_db_initialization, setup
):
    wf = WindowForker(1, 10, setup)
    wf.executors = {
        wf.pool.apply_async(yop, (0,)): (
            "node1",
            "HALT",
            tools.get_date(minimal_db_initialization),
        ),
        wf.pool.apply_async(yop, (0,)): (
            "node2",
            "WAKEUP",
            tools.get_date(minimal_db_initialization),
        ),
        wf.pool.apply_async(yop, (1,)): (
            "node3",
            "HALT",
            tools.get_date(minimal_db_initialization),
        ),
    }
    nodes_list_running = {
        "node1": "command_and_args",
        "node2": "command_and_args",
        "node3": "command_and_args",
    }
    while True:
        wf.check_executors(minimal_db_initialization, setup, nodes_list_running)
        if len(wf.executors) != 3:
            break
    print(nodes_list_running)
    assert nodes_list_running == {
        "node2": "command_and_args",
        "node3": "command_and_args",
    }


@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_window_forker_check_executors_timeout(
    setup_config, setup, minimal_db_initialization
):
    wf = WindowForker(1, 10, setup)
    wf.executors = {wf.pool.apply_async(yop, (0, 0.2)): ("localhost0", "HALT", 0)}
    nodes_list_running = {"localhost0": "command_and_args"}
    wf.check_executors(minimal_db_initialization, setup, nodes_list_running)
    time.sleep(
        0.5
    )  # To prevent deadlock when all tests are executed (due to pytest internals ?)
    print(nodes_list_running)
    assert nodes_list_running == {}
    resource = (
        minimal_db_initialization.query(Resource)
        .filter(Resource.network_address == "localhost0")
        .first()
    )
    assert resource.next_state == "Suspected"
