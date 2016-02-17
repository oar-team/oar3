# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest
from click.testing import CliRunner
from oar.kao.bataar import bataar
import socket
import struct
import sys
import os
from oar.lib import db

recv_msgs = []
sent_msgs = []


class FakeConnection(object):
    msg_idx = 0
    lg_sent = False
    lg_recv = False

    def __init__(self):
        global sent_msgs
        sent_msgs = []

    def recv(self, nb):
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


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_socket_socket():
    socket.socket = FakeSocket


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield


def exec_gene(options):
    global recv_msgs
    recv_msgs = [
        '0:10.000015|10.000015:S:1',
        '0:19.168395|19.168395:C:1'
    ]
    path = os.path.dirname(os.path.abspath(__file__))
    wpf = path + '/batsim-workload.json'
    print(wpf)
    args = [wpf]
    args.extend(options)
    runner = CliRunner()
    result = runner.invoke(bataar, args)
    print("exit code:", result.exit_code)
    print(result.output)
    print("Messages sent:", sent_msgs)
    return (result, sent_msgs)


def test_bataar_no_db():
    result, sent_msgs = exec_gene(['-dno-db'])
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
    assert result.exit_code == 0


def test_bataar_db_memory():
    result, sent_msgs = exec_gene(['-dmemory'])
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
    assert result.exit_code == 0


def test_bataar_db_basic():
    result, sent_msgs = exec_gene(['-pBASIC', '-dmemory'])
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
    assert result.exit_code == 0


def test_bataar_db_local():
    result, sent_msgs = exec_gene(['-pLOCAL', '-n4', '-dmemory'])
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
    assert result.exit_code == 0


def test_bataar_db_best_effort_local():
    result, sent_msgs = exec_gene(['-pBEST_EFFORT_LOCAL', '-n4', '-dmemory'])
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
    assert result.exit_code == 0


def test_bataar_db_contiguous():
    result, sent_msgs = exec_gene(['-pCONTIGUOUS', '-dmemory'])
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
    assert result.exit_code == 0


def test_bataar_db_best_effot_contiguous():
    result, sent_msgs = exec_gene(['-pBEST_EFFORT_CONTIGUOUS', '-dmemory'])
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
    assert result.exit_code == 0
