# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest
from click.testing import CliRunner
from oar.kao.bataar import bataar
import socket
import redis
import struct
import sys
import os
import pdb

from oar.lib import db

recv_msgs = []
sent_msgs = []
data_storage = {}

class FakeConnection(object):
    msg_idx = 0
    lg_sent = False
    lg_recv = False

    def __init__(self):
        global sent_msgs
        sent_msgs = []

    def recv(self, nb):
        #if self.msg_idx == len(recv_msgs):
        #    return None
        if not self.lg_sent:
            self.lg_sent = True
            lg = len(recv_msgs[self.msg_idx])
            return struct.pack("i", int(lg))
        else:
            self.lg_sent = False
            if sys.version_info[0] == 2:
                msg = recv_msgs[self.msg_idx]
            else:
                msg = recv_msgs[self.msg_idx].encode("utf-8")
            self.msg_idx += 1
            return msg

    def sendall(self, msg):
        if not self.lg_recv:
            self.lg_recv = True
        else:
            global sent_msgs
            if sys.version_info[0] == 2:
                sent_msgs.append(msg)
            else:
                # print(type(msg), msg)
                sent_msgs.append(msg.decode("utf-8"))
            self.lg_recv = False


class FakeSocket(object):
    def __init__(self, socket_type, socket_mode):
        pass

    def connect(self, uds_name):
        self.connection = FakeConnection()
        pass

    def recv(self, nb):
        return self.connection.recv(nb)

    def sendall(self, msg):
        return self.connection.sendall(msg)

class FakeRedis(object):
    def __init__(self, host='localchost', port='6379'):
        pass

    def get(self, key):
        return data_storage[key]
    
@pytest.fixture(scope="function", autouse=True)
def monkeypatch_uds_datastorage():
    socket.socket = FakeSocket
    redis.StrictRedis = FakeRedis
    

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield

def exec_gene(options):
    global recv_msgs
    recv_msgs = [
        '0:05|05:A',
        '0:10|10:S:foo!1',
        '0:19|19:C:foo!1',
        '0:25|25:Z'
    ]
    global data_storage
    data_storage = { '/tmp/bat_socket:nb_res': b'4',
                     '/tmp/bat_socket:job_foo!1': b'{"id":"foo!1","subtime":10,"walltime":100,"res":4,"profile":"1"}'
    }
    args = options
    args.append('--scheduler_delay=5')
    runner = CliRunner()
    result = runner.invoke(bataar, args)
    print("exit code:", result.exit_code)
    print(result.output)
    print("Messages sent:", sent_msgs)
    return (result, sent_msgs)


def test_bataar_no_db():
    result, sent_msgs = exec_gene(['-dno-db'])
    assert sent_msgs == ['0:5.000000|5.000000:N', '0:15.000000|15.000000:J:foo!1=0-3',
                         '0:24.000000|24.000000:N', '0:25.000000|25.000000:N']
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_memory():
    result, sent_msgs = exec_gene(['-dmemory'])
    assert sent_msgs == ['0:5.000000|5.000000:N', '0:15.000000|15.000000:J:foo!1=0-3',
                         '0:24.000000|24.000000:N', '0:25.000000|25.000000:N']
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_basic():
    result, sent_msgs = exec_gene(['-pBASIC', '-dmemory'])
    assert sent_msgs == ['0:15.0|15.0:J:1=0-3', '0:24.0|24.0:N', '0:25.000000|25.000000:N']
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_local():
    result, sent_msgs = exec_gene(['-pLOCAL', '-n4', '-dmemory'])
    assert sent_msgs == ['0:15.0|15.0:J:1=0-3', '0:24.0|24.0:N', '0:25.000000|25.000000:N']
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_best_effort_local():
    result, sent_msgs = exec_gene(['-pBEST_EFFORT_LOCAL', '-n4', '-dmemory'])
    assert sent_msgs == ['0:15.0|15.0:J:1=0-3', '0:24.0|24.0:N', '0:25.000000|25.000000:N']
    assert result.exit_code == 0
    
@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_contiguous():
    result, sent_msgs = exec_gene(['-pCONTIGUOUS', '-dmemory'])
    assert sent_msgs == ['0:15.0|15.0:J:1=0-3', '0:24.0|24.0:N', '0:25.000000|25.000000:N']
    assert result.exit_code == 0

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'postgresql'",
                    reason="not designed to work with postgresql database")
def test_bataar_db_best_effot_contiguous():
    result, sent_msgs = exec_gene(['-pBEST_EFFORT_CONTIGUOUS', '-dmemory'])
    assert sent_msgs == ['0:15.0|15.0:J:1=0-3', '0:24.0|24.0:N', '0:25.000000|25.000000:N']
    assert result.exit_code == 0
