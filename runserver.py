#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, unicode_literals

import socket
import argparse
import os

from oar.rest_api.app import create_app


def get_current_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("www.inria.fr", 80))
    ip_address = s.getsockname()[0]
    s.close()
    return ip_address

DEFAULT_BIND = "0.0.0.0"

if os.path.exists("/oar_version"):
    DEFAULT_BIND = get_current_ip()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run application ')
    parser.add_argument('-p', '--port', action="store", default=9090, type=int,
                        help='Set the listening port')
    parser.add_argument('-b', '--bind', action="store", default=DEFAULT_BIND,
                        help='Set the binding address')
    parser.add_argument('--no-debug', action="store_true", default=False,
                        help='Disable debugger')
    args = parser.parse_args()
    app = create_app()
    debug = not args.no_debug
    app.run(host=args.bind, port=args.port, threaded=True, debug=debug)
