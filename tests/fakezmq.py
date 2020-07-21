# coding: utf-8


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class FakeZmqSocket(object):
    def __init__(self, fakezmq):
        print("FakeZmqSocket")
        self.socket_id = fakezmq.num_socket
        self.fakezmq = fakezmq
        self.fakezmq.sent_msgs[self.socket_id] = []

    def setsockopt(self, x, y):
        pass

    def bind(self, url):
        pass

    def connect(self, url):
        pass

    def close(self):
        pass

    def send(self, msg):
        print("send", self.socket_id, msg)
        self.fakezmq.sent_msgs[self.socket_id].append(msg)

    def send_string(self, msg):
        print("send_string", self.socket_id, msg)
        self.fakezmq.sent_msgs[self.socket_id].append(msg)

    def send_json(self, msg):
        print("send_json", self.socket_id, msg)
        self.fakezmq.sent_msgs[self.socket_id].append(msg)

    def _pop_msg(self):
        msgs = self.fakezmq.recv_msgs[self.socket_id]
        if len(msgs) == 0:
            msg = None
        else:
            # msg = FakeZmqSocketMessage(msgs.pop(0))
            msg = msgs.pop(0)
            if type(msg) is dict:
                return msg
            else:
                return msg.encode("utf8")

    def recv_json(self):
        return self._pop_msg()

    def recv(self):
        return self.recv_json()

    def recv_string(self):
        return self.recv_json()

    def recv_multipart(self):
        print("recv_multipart:", self.socket_id, self.fakezmq.recv_msgs)
        msg = self._pop_msg()
        client_id = 1
        return (client_id, msg)


class FakeZmq(metaclass=Singleton):
    def __init__(self):
        self.num_socket = 0
        self.sent_msgs = {}
        self.recv_msgs = {}

    def reset(self):
        self.num_socket = 0
        self.sent_msgs = {}
        self.recv_msgs = {}

    def socket(self, _):
        print("Create socket: {}".format(self.num_socket))
        sock = FakeZmqSocket(self)
        self.num_socket += 1
        return sock
