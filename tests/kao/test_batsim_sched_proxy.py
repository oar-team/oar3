# coding: utf-8
import pytest
import redis
import zmq

import oar.lib.tools  # for monkeypatching
from oar.kao.meta_sched import meta_schedule
from oar.lib.job_handling import insert_job
from oar.lib.tools import get_date

from ..fakezmq import FakeZmq

data_store = {}

fakezmq = FakeZmq()


class FakeRedis(object):
    def __init__(self, host="localchost", port="6379"):
        pass

    def set(self, key, value):
        data_store[key] = bytes(value, "utf8")

    def get(self, key):
        return data_store[key]


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_datastorage():
    redis.StrictRedis = FakeRedis


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config["BATSCHED_ENDPOINT"] = "tcp://localhost:6679"
    config["DS_PREFIX"] = "oar"
    config["WLOAD_BATSIM"] = "oar"
    fakezmq.reset()

    @request.addfinalizer
    def teardown():
        del config["BATSCHED_ENDPOINT"]
        del config["DS_PREFIX"]
        del config["WLOAD_BATSIM"]


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db["Queue"].create(
            name="default", priority=3, scheduler_policy="kamelot", state="Active"
        )

        # add some resources
        for i in range(5):
            db["Resource"].create(network_address="localhost")
        yield


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda x, y: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "notify_tcp_socket", lambda addr, port, msg: len(msg)
    )
    monkeypatch.setattr(
        oar.lib.tools, "notify_user", lambda job, state, msg: len(state + msg)
    )
    monkeypatch.setattr(oar.lib.tools, "notify_bipbip_commander", lambda json_msg: True)
    monkeypatch.setattr(zmq, "Context", FakeZmq)


@pytest.mark.skip(
    reason="Needs batsim which is currently disabled because of a pybatsim installation issue with poetry"
)
def test_simple_submission(monkeypatch):
    now = str(get_date())

    # 2:1484687842.0|1484687842.0:S:1
    print("DB_BASE_FILE: ", config["DB_BASE_FILE"])
    insert_job(res=[(60, [("resource_id=4", "")])], properties="")
    job = db["Job"].query.one()
    print("job state:", job.state)

    msg = (
        '{"now":'
        + now
        + ', "events": [{"timestamp": '
        + now
        + ', "type": "EXECUTE_JOB", "data": {"job_id": "oar!'
        + str(job.id)
        + '", "alloc": "1-4"}}]}'
    )

    fakezmq.recv_msgs = {0: [msg]}

    meta_schedule("batsim_sched_proxy")

    for i in db["GanttJobsPrediction"].query.all():
        print("moldable_id: ", i.moldable_id, " start_time: ", i.start_time)

    job = db["Job"].query.one()
    print(job.state)
    assert job.state == "toLaunch"
