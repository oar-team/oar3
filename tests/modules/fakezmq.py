# coding: utf-8
from __future__ import unicode_literals, print_function

class FakeZmqSocketMessage(object):
    def __init__(self, msg):
        print("FakeZmqSocketMessage", msg)
        self.msg = msg

    def decode(self, fmt):
        return self.msg

    
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
        print("send", self.num, msg)
        FakeZmq.sent_msgs[self.num].append(msg)

    def send_string(self, msg):
        print("send_string", self.num, msg)
        FakeZmq.sent_msgs[self.num].append(msg)

    def send_json(self, msg):
        print("send_json", self.num, msg)
        FakeZmq.sent_msgs[self.num].append(msg)

    def recv_json(self):
        msgs = FakeZmq.recv_msgs[self.num]
        if len(msgs) == 0:
            msg = None
        else:
            msg = msgs.pop()
        return msg

    def recv(self):
        return self.recv_json()

    def recv_multipart(self):
        print('recv_multipart:', self.num, FakeZmq.recv_msgs)
        msgs = FakeZmq.recv_msgs[self.num]
        if len(msgs) == 0:
            msg = None
        else:
            msg = FakeZmqSocketMessage(msgs.pop())
        client_id = 1
        return(client_id, msg)


class FakeZmq(object):
    i = 0
    sent_msgs = {}
    recv_msgs = {}
    def __init__(self):
        pass

    def socket(self, socket_type):
        sock = FakeZmqSocket(FakeZmq.i)
        FakeZmq.i += 1
        return sock
