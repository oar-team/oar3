#!/usr/bin/env python
# coding: utf-8
"""Proxy to help incremental transition toward ZMQ use 
between version 2.x OAR's modules and version 3.x
"""

import re
import zmq
from oar.lib import (config, get_logger)


# Set undefined config value to default one
DEFAULT_CONFIG = {
    'SERVER_HOSTNAME': 'localhost',
    'SERVER_PORT': '6666',
    'APPENDICE_PROXY_SERVER_PORT': '6668',
    'BIPBIP_COMMANDER_SERVER': 'localhost',
    'BIPBIP_COMMANDER_PORT': '6669',
}

config.setdefault_config(DEFAULT_CONFIG)


OAR_EXEC_RUNJOB_LEON = r'(OAREXEC|OARRUNJOB|LEONEXTERMINATE)_(\d+)(.*)'
# Regexp of the notification received from oarexec processes
#   $1: OAREXEC|OARRUNJOB|LEONEXTERMINATE
#   $2: job id
#   $3: for OAREXEC: oarexec exit code, job script exit code,
#                    secret string that identifies the oarexec process (for security)

logger = get_logger("oar.modules.appendice_proxy", forward_stderr=True)
logger.info('Start Appendice Proxy')

class AppendiceProxy(object):
    def __init__(self):
        self.context = zmq.Context()
        self.socket_proxy = self.context.socket(zmq.STREAM)
        self.socket_proxy.bind("tcp://*:" + str(config['SERVER_PORT']))

        self.appendice = self.context.socket(zmq.PUSH)
        self.appendice.connect("tcp://" + config['SERVER_HOSTNAME'] + ":"
                               + config['APPENDICE_PROXY_SERVER_PORT'])

        self.bipbip_commander = self.context.socket(zmq.PUSH)
        self.bipbip_commander.connect("tcp://" + config['BIPBIP_COMMANDER_SERVER'] + ":"
                                      + config['BIPBIP_COMMANDER_PORT'])


    def run(self, loop=True):
        while True:
            client_id, message = self.socket_proxy.recv_multipart()
            msg=message.decode('utf8')
            
            print("id: %r" % client_id)
            print("request: %s" % message.decode('utf8'))
            
            # if OAREXEC or OARRUNJOB or LEONEXTERMINATE is received forward it to bipbip commander 
            m = re.search(OAR_EXEC_RUNJOB_LEON,  msg)
            if m:
                command = m.group(1)
                job_id = m.group(2)
                args= m.group(3).split('_')[1:]
                self.bipbip_commander.send_json({'job_id': int(job_id), 'cmd': command, 'args': args})

            else:
                self.appendice.send_json({'msg': message.decode('utf8')})
            
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

if __name__ == '__main__':  # pragma: no cover
    appendice_proxy = AppendiceProxy()
    appendice_proxy.run()
