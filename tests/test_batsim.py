# coding: utf-8
from __future__ import unicode_literals, print_function
from oar.kao.batsim import main
import pytest
import socket
import struct
import sys
import os
from oar.lib import (db, config)

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


def test_batsim_no_db_1():

    global recv_msgs
    recv_msgs = [
        '0:10.000015|10.000015:S:1',
        '0:19.168395|19.168395:C:1'
    ]
    path = os.path.dirname(os.path.abspath(__file__))
    main(path + '/batsim-workload.json', 'no-db')
    print("Messages sent:", sent_msgs)
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']


def _test_batsim_db_memory_1():  # TODO DEBUG
    db.delete_all()
    db.session.close()
    global recv_msgs
    recv_msgs = [
        '0:10.000015|10.000015:S:1',
        '0:19.168395|19.168395:C:1'
    ]
    path = os.path.dirname(os.path.abspath(__file__))
    main(path + '/batsim-workload.json', 'memory')

    print("Messages sent:", sent_msgs)
    assert sent_msgs == ['0:15|15:J:1=0,1,2,3', '0:24|24:N']
