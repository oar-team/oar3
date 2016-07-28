# coding: utf-8
from __future__ import unicode_literals, print_function

class FakeZmqSocketMessage(object):
    def __init__(self, msg):
        print("FakeZmqSocketMessage", msg)
        self.msg = msg

    def decode(self, fmt):
        return(self.msg)

class FakeZmqSocket(object):
    def __init__(self, num):
        print("FakeZmqSocket")
        self.num = num
        FakeZmq.sent_msgs[num] = []

    def bind(self, url):
        pass

    def connect(self, url):
        pass

    def send(self, msg):
        print("send",self.num, msg)
        FakeZmq.sent_msgs[self.num].append(msg)

    def send_json(self, msg):
        print("send_json",self.num, msg)
        FakeZmq.sent_msgs[self.num].append(msg)

    def recv_json(self):
        return FakeZmq.recv_msgs[self.num].pop()
        
    def recv_multipart(self):
        print('recv_multipart:', self.num, FakeZmq.recv_msgs)
        msg = FakeZmqSocketMessage(FakeZmq.recv_msgs[self.num].pop())
        client_id = 1
        return(client_id, msg)


class FakeZmq(object):
    i = 0
    sent_msgs = {}
    recv_msgs = {}
    def __init__(self):
        pass

    def socket(self, socket_type):
        s = FakeZmqSocket(FakeZmq.i)
        FakeZmq.i += 1
        return s
