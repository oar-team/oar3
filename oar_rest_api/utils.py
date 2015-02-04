# -*- coding: utf-8 -*-
from __future__ import division

import time
import json
import decimal
import datetime


class WSGIProxyFix(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        user = environ.pop('X_REMOTE_IDENT', None)
        environ['USER'] = user
        return self.app(environ, start_response)


class JSONEncoder(json.JSONEncoder):
    """JSON Encoder class that handles conversion for a number of types not
    supported by the default json library, especially the sqlalchemy objects.

    :returns: object that can be converted to json
    """

    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        elif isinstance(obj, (decimal.Decimal)):
            return unicode(obj)
        elif hasattr(obj, '_asdict') and callable(getattr(obj, '_asdict')):
            return obj._asdict()
        elif hasattr(obj, 'asdict') and callable(getattr(obj, 'asdict')):
            return obj.asdict()
        else:
            return json.JSONEncoder.default(self, obj)


def get_utc_timestamp():
    return int(time.time())
