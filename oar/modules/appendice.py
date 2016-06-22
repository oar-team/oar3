#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals, print_function

from oar.lib import (config, get_logger)
import os
import socket

# Set undefined config value to default one
DEFAULT_CONFIG = {
    'SERVER_HOSTNAME': 'localhost',
    'SERVER_PORT': 6666
}
servermaxconnect = 100


# This timeout is used by appendice to prevent a client to block
# reception by letting a connection opened
# should be left at a positive value
appendice_connection_timeout = 5

config.setdefault_config(DEFAULT_CONFIG)
logger = get_logger('oar.modules.appendice', forward_stderr=True)


if 'OARDIR' in os.environ:
    binpath = os.environ['OARDIR'] + '/'
else:
    binpath = '/usr/local/lib/oar/'
    logger.warning("OARDIR env variable must be defined, " + binpath + " is used by default")


def main():
    logger.info('Start appendice')   
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((config['SERVER_HOSTNAME'], config['SERVER_PORT']))
    server.listen(servermaxconnect)

    while True:

        conn, addr = server.accept()
        conn.settimeout(appendice_connection_timeout)
        answer = 'A'
        while True:
            try:
                recv = conn.recv()
            except socket.Timeouterror:
                logger.error('socket timeout error from connection with' + addr)
                break
        if recv == '':
            logger.warn('receive null string from ' + addr)

        answer += recv
        if recv.endswith('\n'):
            # Do we need to clean up theanswer ? Below code extracted from perl version
            # cleans the answer of all unwanted trailing characters
            # while ($answer && $carac !~ '[a-zA-Z0-9]'){
            #    $carac=chop $answer;
            # }
            print(answer[:-1])
            break

        #  TODO comportement_appendice
        # def comportement_appendice():  # TODO
        '''main body of the appendice, a forked process dedicated
        to the listening of commands
        the interest of such a forked process is to ensure that clients get their
        notification as soon as possible (i.e. reactivity) even if the almighty is
        performing some other internal task in the meantime
        '''
        # bipbip_launcher_pid=fork();
    
        answer += recv
        if recv.endswith('\n'):
            # Do we need to clean up the answer ? Below code extracted from perl version
            # cleans the answer of all unwanted trailing characters
            # while ($answer && $carac !~ '[a-zA-Z0-9]'){
            #    $carac=chop $answer;
            # }
            print(answer[:-1])
            break

if __name__ == '__main__':  # pragma: no cover
    main()
