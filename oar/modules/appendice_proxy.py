#!/usr/bin/env python
# coding: utf-8
"""Proxy to help incremental transition toward ZMQ use 
between version 2.x OAR's modules and version 3.x
"""

import zmq
from oar.lib import (config, get_logger)


# Set undefined config value to default one
DEFAULT_CONFIG = {
    'SERVER_HOSTNAME': 'localhost',
    'SERVER_PORT': '6666',
    'APPENDICE_PROXY_SERVER_PORT': '6668',
}

config.setdefault_config(DEFAULT_CONFIG)

class AppendiceProxy(object):
    def __init__(self):
        self.context = zmq.Context()
        self.socket_proxy = self.context.socket(zmq.STREAM)
        self.socket_proxy.bind("tcp://*:" + str(config['SERVER_PORT']))

        self.appendice = self.context.socket(zmq.PUSH)
        self.appendice.connect("tcp://" + config['SERVER_HOSTNAME'] + ":"
                           + config['APPENDICE_PROXY_SERVER_PORT'])

    def run(self, loop=True):
        while True:
            client_id, message = self.socket_proxy.recv_multipart()

            print("id: %r" % client_id)
            print("request: %s" % message.decode('utf8'))

            self.appendice.send(message)
            if not loop:
                break

    #      context = zmq.Context()
    #socket = context.socket(zmq.ROUTER)
    #socket.bind("tcp://127.0.0.1:%i" % port)

    #while True:
    #    message = socket.recv_multipart()
    #    req_id = message[0]
    #    print("Received request: %s" % str(type(message[1:][0])))
    #    print("message: %s" % message[1:][0].decode('utf8'))
    #    time.sleep(1)

def main():
    appendice_proxy = AppendiceProxy()
    appendice_proxy.run()

if __name__ == '__main__':  # pragma: no cover
    main()
