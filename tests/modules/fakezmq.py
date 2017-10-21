# coding: utf-8

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
            #msg = FakeZmqSocketMessage(msgs.pop(0))
            msg = msgs.pop(0)
            if type(msg) is dict:
                return msg
            else:
                return msg.encode('utf8')

    def recv_json(self):
        return self._pop_msg()

    def recv(self):
        return self.recv_json()

    def recv_string(self):
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
    
    @classmethod
    def reset(cls):
        FakeZmq.num_socket = 0
        FakeZmq.sent_msgs = {}
        FakeZmq.recv_msgs = {}
        
    @classmethod
    def socket(cls, _):
        sock = FakeZmqSocket(FakeZmq.num_socket)
        FakeZmq.num_socket += 1
        return sock
