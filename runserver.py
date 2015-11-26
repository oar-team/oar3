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


def parse_proxy_url(url):
    """Parses strings in the form host[:port]/path"""
    host_prefix = url.split('/', 1)
    if len(host_prefix) == 1:
        prefix = ""
        h = host_prefix[0]
    else:
        prefix = host_prefix[1]
        h = host_prefix[0]

    host_port = h.split(":", 1)
    if len(host_port) == 1:
        v = (h, 80, prefix)
    else:
        v = host_port[0], int(host_port[1]), prefix
    return {'proxy_host': v[0], 'proxy_port': v[1], 'proxy_prefix': v[2]}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run application ',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('-p', '--port', action="store", default=9090, type=int,
                        help='Set the listening port')
    parser.add_argument('-b', '--bind', action="store", default=DEFAULT_BIND,
                        help='Set the binding address')
    parser.add_argument('--no-debug', action="store_true", default=False,
                        help='Disable debugger')
    parser.add_argument('--old-api-proxy', action="store",
                        default="localhost:80/oarapi",
                        help='Set the binding address')
    args = parser.parse_args()
    # Disable PIN (Never enable the debugger in production!)
    os.environ['WERKZEUG_DEBUG_PIN'] = 'off'
    proxy_kwargs = parse_proxy_url(args.old_api_proxy)
    app = create_app(**proxy_kwargs)
    debug = not args.no_debug
    app.run(host=args.bind, port=args.port, threaded=True, debug=debug)
