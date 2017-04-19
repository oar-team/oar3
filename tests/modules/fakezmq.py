# coding: utf-8
from __future__ import unicode_literals, print_function

class FakeZmqSocketMessage(object):
    def __init__(self, msg):
        print("FakeZmqSocketMessage", msg)
        self.msg = msg

    def decode(self, fmt):
        return self.msg

    
class FakeZmqSocket(object):
    def __init__(self, socket_id):
        print("FakeZmqSocket")
        self.socket_id = socket_id
        FakeZmq.sent_msgs[socket_id] = []

    def bind(self, url):
        pass

    def connect(self, url):
        pass

    def close(self):
        pass

    def send(self, msg):
        print("send", self.socket_id, msg)
        FakeZmq.sent_msgs[self.socket_id].append(msg)

    def send_string(self, msg):
        print("send_string", self.socket_id, msg)
        FakeZmq.sent_msgs[self.socket_id].append(msg)

    def send_json(self, msg):
        print("send_json", self.socket_id, msg)
        FakeZmq.sent_msgs[self.socket_id].append(msg)

    def _pop_msg(self):
        msgs = FakeZmq.recv_msgs[self.socket_id]
        if len(msgs) == 0:
            msg = None
        else:
            msg = FakeZmqSocketMessage(msgs.pop(0))
        return msg 
        
    def recv_json(self):
        return self._pop_msg()

    def recv(self):
        return self.recv_json()

    def recv_multipart(self):
        print('recv_multipart:', self.socket_id, FakeZmq.recv_msgs)
        msg = self._pop_msg()
        client_id = 1
        return(client_id, msg)


class FakeZmq(object):
    num_socket = 0
    sent_msgs = {}
    recv_msgs = {}
    def __init__(self):
        pass

    def socket(self, socket_type):
        sock = FakeZmqSocket(FakeZmq.num_socket)
        FakeZmq.num_socket += 1
        return sock
