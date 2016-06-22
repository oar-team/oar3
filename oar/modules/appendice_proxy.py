#!/usr/bin/env python
# coding: utf-8
"""Proxy to help incremental transition toward ZMQ use 
between version 2.x OAR's modules and version 3.x
"""

from oar.lib import (config, get_logger)
import zmq

# Set undefined config value to default one
DEFAULT_CONFIG = {
    'META_SCHED_CMD': 'kao',
    'SERVER_HOSTNAME': 'localhost',
    'SERVER_PORT': '6666',
    'APPENDICE_PROXY_SERVER_PORT': '6668',
}
config.setdefault_config(DEFAULT_CONFIG)

if __name__ == '__main__':

    context = zmq.Context()
    socket = context.socket(zmq.STREAM)
    socket.bind("tcp://*:" + config[SERVER_PORT])

    appendice_proxy = context.socket(zmq.PUSH)
    appendice_proxy.connect("tcp://" + config['SERVER_HOSTNAME'] + ":"
                       + config['APPENDICE_PROXY_SERVER_PORT'])

    while True:
        clientid, message = socket.recv_multipart()

        print("id: %r" % clientid)
        print("request: %s" % message.decode('utf8'))

        appendice_proxy.send(message)



          context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.bind("tcp://127.0.0.1:%i" % port)

    while True:
        message = socket.recv_multipart()
        req_id = message[0]
        print("Received request: %s" % str(type(message[1:][0])))
        print("message: %s" % message[1:][0].decode('utf8'))
        time.sleep(1)
