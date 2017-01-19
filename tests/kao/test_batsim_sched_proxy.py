# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest
from ..modules.fakezmq import FakeZmq
#import socket
import redis
import zmq

from oar.lib import config, db
from oar.lib.tools import get_date
from oar.kao.job import insert_job
from oar.kao.meta_sched import meta_schedule

import oar.lib.tools  # for monkeypatching
from oar.lib.tools import get_date

data_store = {}

class FakeRedis(object):
    def __init__(self, host='localchost', port='6379'):
        pass

    def set(self, key, value):
        data_store[key] = value
    
    def get(self, key):
        return data_store[key]
    
@pytest.fixture(scope="function", autouse=True)
def monkeypatch_datastorage():
    redis.StrictRedis = FakeRedis

@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config['BATSCHED_ENDPOINT'] = 'tcp://localhost:6679'
    config['DS_PREFIX'] = 'oar'
    config['WLOAD_BATSIM'] = 'oar'

    @request.addfinalizer
    def teardown():
        del config['BATSCHED_ENDPOINT']
        del config['DS_PREFIX']
        config['WLOAD_BATSIM']
        FakeZmq.i = 0
        FakeZmq.sent_msgs = {}
        FakeZmq.recv_msgs = {}

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')

        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost")
        yield


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'init_judas_notify_user', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: len(x))
    monkeypatch.setattr(oar.lib.tools, 'notify_tcp_socket', lambda addr, port, msg: len(msg))
    monkeypatch.setattr(oar.lib.tools, 'notify_user', lambda job, state, msg: len(state + msg))
    monkeypatch.setattr(zmq, 'Context', FakeZmq)
    

def test_simple_submission(monkeypatch):

    now = str(get_date())
    
    #2:1484687842.0|1484687842.0:S:1
    print("DB_BASE_FILE: ", config["DB_BASE_FILE"])
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    job = db['Job'].query.one()
    print('job state:', job.state)

    FakeZmq.recv_msgs[0] = ['2:' + now + '|' + now + ':J:oar!' + str(job.id) +'=1-4']
    
    meta_schedule('batsim_sched_proxy')

    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    job = db['Job'].query.one()
    print(job.state)
    assert (job.state == 'toLaunch')

