# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest
from ..modules.fakezmq import FakeZmq
from click.testing import CliRunner

import redis
import zmq
import struct
import sys
import os
import json

from oar.lib import db

if sys.version_info >= (3,4):
    from oar.kao.bataar import bataar

pytestmark = pytest.mark.skipif(sys.version_info < (3,4) , reason='Bataar need Python 3')


def order_json_str_arrays(a):
    return [json.dumps(json.loads(x), sort_keys=True) for x in a]

SENT_MSGS_1 = order_json_str_arrays([
    '{"now": 5.0, "events": []}',
    '{"now": 15.0, "events": [{"type": "EXECUTE_JOB", "data": {"alloc": "0 - 3", "job_id": "foo!1"}, "timestamp": 15.0}]}',
    '{"now": 24.0, "events": []}',
    '{"now": 25.0, "events": []}'
])

data_storage = {}

class FakeRedis(object):
    def __init__(self, host='localhost', port='6379'):
        pass

    def get(self, key):
        return data_storage[key]
    
@pytest.fixture(scope="function", autouse=True)
def monkeypatch_datastore_zmq():
    redis.StrictRedis = FakeRedis
    zmq.Context = FakeZmq
    #monkeypatch.setattr(zmq, 'Context', FakeZmq)

@pytest.fixture(scope="function", autouse=True)
def setup(request):
    FakeZmq.reset()

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield

def exec_gene(options):
    FakeZmq.recv_msgs = {0:[
        '{"now":5.0, "events":\
[{"timestamp":5.0,"type": "SIMULATION_BEGINS","data":{"nb_resources":4,"config":\
{"redis": {"enabled": true, "hostname": "localhost", "port": 6379, "prefix": "default"}}}}]}',
'{"now":10.0, "events":\
[{"timestamp":10.0,"type": "JOB_SUBMITTED", "data": {"job_id": "foo!1"}}]}',
'{"now":19.0, "events":\
[{"timestamp":19.0, "type":"JOB_COMPLETED","data":{"job_id":"foo!1","status":"SUCCESS"}}]}',
'{"now":25.0, "events":\
[{"timestamp":25.0, "type": "SIMULATION_ENDS", "data": {}}]}'
    ]}
    
    global data_storage
    data_storage = { 'default:job_foo!1': b'{"id":"foo!1","subtime":10,"walltime":100,"res":4,"profile":"1"}'
    }
    args = options
    args.append('--scheduler_delay=5')
    runner = CliRunner()

    result = runner.invoke(bataar, args)
    print("exit code:", result.exit_code)
    print(result.output)
    print("Messages sent:", FakeZmq.sent_msgs)
    return (result,  FakeZmq.sent_msgs)

@pytest.mark.skip(reason='need lastest version pybatsim ')
def test_bataar_no_db():    
    result, sent_msgs = exec_gene(['-dno-db'])
    
    assert order_json_str_arrays(sent_msgs[0]) == SENT_MSGS_1
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")

def test_bataar_db_memory():
    result, sent_msgs = exec_gene(['-dmemory'])
    job = db['Job'].query.one()
    print(job.id, job.state)

    assert order_json_str_arrays(sent_msgs[0]) == SENT_MSGS_1
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_basic():
    result, sent_msgs = exec_gene(['-pBASIC', '-dmemory'])

    assert order_json_str_arrays(sent_msgs[0]) == SENT_MSGS_1
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_local(): #TODO need better test to verify allocation policy 
    result, sent_msgs = exec_gene(['-pLOCAL', '-n4', '-dmemory'])

    assert order_json_str_arrays(sent_msgs[0]) == SENT_MSGS_1
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_best_effort_local(): #TODO need better test to verify allocation policy 
    result, sent_msgs = exec_gene(['-pBEST_EFFORT_LOCAL', '-n4', '-dmemory'])

    assert order_json_str_arrays(sent_msgs[0]) == SENT_MSGS_1
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_contiguous(): #TODO need better test to verify allocation policy 
    result, sent_msgs = exec_gene(['-pCONTIGUOUS', '-dmemory'])

    assert order_json_str_arrays(sent_msgs[0]) == SENT_MSGS_1
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_best_effort_contiguous(): #TODO need better test to verify allocation policy 
    result, sent_msgs = exec_gene(['-pBEST_EFFORT_CONTIGUOUS', '-dmemory'])

    assert order_json_str_arrays(sent_msgs[0]) == SENT_MSGS_1
    assert result.exit_code == 0
