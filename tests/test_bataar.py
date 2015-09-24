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

    def bind(self, name):
        pass

    def listen(self, nb):
        pass

    def accept(self):
        return(FakeConnection(), None)


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_socket_socket():
    socket.socket = FakeSocket


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield


def test_bataar_no_db_1():

    global recv_msgs
    recv_msgs = [
        '0:10.000015|10.000015:S:1',
        '0:19.168395|19.168395:C:1'
    ]
    path = os.path.dirname(os.path.abspath(__file__))
    wpf = path + '/batsim-workload.json'
    print(wpf)
    runner = CliRunner()
    result = runner.invoke(bataar, [wpf, '-dno-db'])
    print(result.exit_code)
    # print(result.output)
    print("Messages sent:", sent_msgs)
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
    assert result.exit_code == 0


def test_bataar_db():
    global recv_msgs
    recv_msgs = [
        '0:10.000015|10.000015:S:1',
        '0:19.168395|19.168395:C:1'
    ]

    path = os.path.dirname(os.path.abspath(__file__))
    wpf = path + '/batsim-workload.json'

    runner = CliRunner()
    result = runner.invoke(bataar, [wpf, '-dmemory'])
    print(result.exit_code)

    print("Messages sent:", sent_msgs)
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
