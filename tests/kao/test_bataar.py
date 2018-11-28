# coding: utf-8
import pytest
from ..fakezmq import FakeZmq
from click.testing import CliRunner

import redis
import zmq
import struct
import sys
import os
import json
from oar.lib import db

from oar.kao.bataar import bataar

def order_json_str_arrays(a):
    return [json.dumps(json.loads(x), sort_keys=True) for x in a]

fakezmq = FakeZmq()

SENT_MSGS_1 = order_json_str_arrays(['{"now": 5.0, "events": []}',
                                     '{"now": 15.0, "events": [{"timestamp": 15.0, "type": "EXECUTE_JOB", "data": {"job_id": "foo!1", "alloc": "0-3"}}]}',
                                     '{"now": 24.0, "events": []}',
                                     '{"now": 25.0, "events": []}'
])

SENT_MSGS_2 = order_json_str_arrays([
    '{"now": 5.0, "events": []}',
    '{"now": 12.0, "events": [{"timestamp": 11.0, "data": {"job_id": "foo!1", "alloc": "0 - 1"}, "type": "EXECUTE_JOB"}, {"timestamp": 12.0, "data": {"job_id": "foo!3", "alloc": "2 - 3"}, "type": "EXECUTE_JOB"}]}',
    '{"now": 19.5, "events": [{"timestamp": 19.5, "data": {"job_id": "foo!2", "alloc": "0 - 1"}, "type": "EXECUTE_JOB"}]}',
    '{"now": 25.0, "events": []}'])

data_storage = {}

class FakeRedis(object):
    def __init__(self, host='localhost', port='6379'):
        pass

    def get(self, key):
        #import pdb; pdb.set_trace()
        return data_storage[key]
    
@pytest.fixture(scope="function", autouse=True)
def monkeypatch_datastore_zmq():
    redis.StrictRedis = FakeRedis
    zmq.Context = FakeZmq
    #monkeypatch.setattr(zmq, 'Context', FakeZmq)

@pytest.fixture(scope="function", autouse=True)
def setup(request):
    fakezmq.reset()

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield

BASE_SIMU_MSGS = [
    {'now':5.0,
     'events': [
         {'timestamp':5.0,
          'type': 'SIMULATION_BEGINS',
          'data':{
              'nb_resources':4,  #TODO REMOVE
              'nb_compute_resources':4,
              'nb_storage_resources':0,
              'compute_resources': [
                  {'id':0, 'name': 'node1'},
                  {'id':1, 'name': 'node2'},
                  {'id':2, 'name': 'node3'},
                  {'id':3, 'name': 'node4'}
              ],
              'storage_resources': [],
              'allow_time_sharing': False, #TODO REMOVE
              'allow_time_sharing_on_compute': False,
              'allow_time_sharing_on_storage': False,
              'config': {
                  'profiles-forwarded-on-submission': False,
                  'dynamic-jobs-enabled': False,
                  'dynamic-jobs-acknowledged': False,
                  'redis-enabled': True,
                  'redis-hostname': 'localhost',
                  'redis-port': 6379,
                  'redis-prefix': 'default',
                  'redis':{ #TODO REMOVE
                      'enabled': True, 'hostname': 'localhost',
                      'port': 6379, 'prefix': 'default'
                  },
                  'job_submission': {'forward_profiles': False,
                                 'from_scheduler': {'enabled': False, 'acknowledge': True}
                  }
              },
              'workloads': {},
              'profiles': {'foo':{}} #TODO
          }
         }
     ]
    },
    {'now': 10.0,
     'events': [
         {'timestamp': 10.0, 'type': 'JOB_SUBMITTED',
          'data': {'job_id': 'foo!1'}
         }
     ]
    },
    {'now': 19.0,
     'events': [
         {'timestamp': 19.0, 'type':'JOB_COMPLETED',
          'data':{
              'job_id':'foo!1', 'status':'SUCCESS',
              'job_state': 'terminated', 'kill_reason': 'none', 'return_code':0
          }
         }
     ]},
    {'now': 25.0,
     'events': [
         {'timestamp':25.0, 'type': 'SIMULATION_ENDS',
          'data': {}
         }
     ]}
    ]

def exec_gene(options):
    fakezmq.recv_msgs = {0:[json.dumps(msg) for msg in BASE_SIMU_MSGS]}
    #import pdb; pdb.set_trace()
    global data_storage
    data_storage = { 'default:job_foo!1': b'{"id":"foo!1","subtime":10,"walltime":100,"res":4,"profile":"1"}',
                     'default:profile_foo!1': b'{"type":"delay", "runtime": 120}'}
    args = options
    args.append('--scheduler_delay=5')
    runner = CliRunner()

    result = runner.invoke(bataar, args)
    print("exit code:", result.exit_code)
    print(result.output)
    print("Messages sent:", fakezmq.sent_msgs)
    return (result,  fakezmq.sent_msgs)

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



def exec_gene_tokens(options):
    fakezmq.recv_msgs = {0:[
        '{"now":5.0, "events":\
[{"timestamp":5.0,"type": "SIMULATION_BEGINS","data":{"nb_resources":4,"config":\
{"redis": {"enabled": true, "hostname": "localhost", "port": 6379, "prefix": "default"}}}}]}',
'{"now":10.5, "events":\
[{"timestamp":10.0,"type": "JOB_SUBMITTED", "data": {"job_id": "foo!1"}},\
{"timestamp":10.1,"type": "JOB_SUBMITTED", "data": {"job_id": "foo!2"}},\
{"timestamp":10.2,"type": "JOB_SUBMITTED", "data": {"job_id": "foo!3"}}]}',
'{"now":19.0, "events":\
[{"timestamp":19.0, "type":"JOB_COMPLETED","data":{"job_id":"foo!1","status":"SUCCESS"}}]}',
'{"now":25.0, "events":\
[{"timestamp":25.0, "type": "SIMULATION_ENDS", "data": {}}]}'
    ]}
    
    global data_storage
    data_storage = { 'default:job_foo!1':
                     b'{"id":"foo!1","subtime":10,"walltime":100,"res":2,"tokens":3,"profile":"1"}',
                     'default:job_foo!2':
                     b'{"id":"foo!2","subtime":10,"walltime":100,"res":2,"tokens":2,"profile":"1"}',
                     'default:job_foo!3':
                     b'{"id":"foo!3","subtime":10,"walltime":100,"res":2,"profile":"1"}',
                     'profile_foo!1':
                     b'{"command": "foo","delay": 290.53, "runtime": "type": "delay"}'
    }
    args = options
    args.append('--scheduler_delay=0.5')
    runner = CliRunner()
    #import pdb; pdb.set_trace()
    result = runner.invoke(bataar, args)
    print("exit code:", result.exit_code)
    print(result.output)
    return (result,  fakezmq.sent_msgs)

@pytest.mark.skip(reason='Bug pending........................')
def test_bataar_tokens_no_db():

    result, sent_msgs = exec_gene_tokens(['-dno-db', '--tokens=4'])
    print("Messages sent:", sent_msgs)
    assert order_json_str_arrays(sent_msgs[0]) == SENT_MSGS_2
    assert result.exit_code == 0
