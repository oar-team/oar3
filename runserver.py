#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, unicode_literals

import argparse

from oar.rest_api.app import create_app


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run application ')
    parser.add_argument('-p', '--port', action="store", default=9090, type=int,
                        help='Set the listening port')
    parser.add_argument('-b', '--bind', action="store", default="0.0.0.0",
                        help='Set the binding address')

    args = parser.parse_args()
    app = create_app()
    app.run(host=args.bind, port=args.port, threaded=True, debug=True)
