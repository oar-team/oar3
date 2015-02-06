# -*- coding: utf-8 -*-
from __future__ import division

import sys
import json
import decimal
import datetime

from flask import request, abort
from oar.lib.compat import reraise, to_unicode, iteritems


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


class ArgParser(object):
    """Flask request argument parser."""

    DEFAULT_TARGETS = ('querystring', 'form', 'json')

    MISSING = object()

    def __init__(self, argmap):
        self.argmap = argmap

    def get_value(self, data, name):
        return data.get(name, self.MISSING)

    def parse_value(self, name):
        """Pull a form value from the request."""
        for target in self.DEFAULT_TARGETS:
            if target == "querystring":
                value = self.get_value(request.args, name)
            elif target == "json":
                json_data = request.get_json(silent=True, force=True)
                if json_data:
                    value = self.get_value(json_data, name)
                else:
                    value = self.MISSING
            elif target == "form":
                value = self.get_value(request.form, name)
            if value is not self.MISSING:
                return value
        return self.MISSING

    def convert(self, value, argtype):
        if argtype == str:
            return to_unicode(value)
        else:
            return argtype(value)

    def parse(self):
        """Parses the request arguments."""
        kwargs = {}
        for argname, argtype in iteritems(self.argmap):
            value = self.parse_value(argname)
            if value is not self.MISSING:
                try:
                    kwargs[argname] = self.convert(value, argtype)
                except:
                    try:
                        abort(400)
                    except:
                        exc_type, exc_value, tb = sys.exc_info()
                        exc_value.data = \
                            ("The parameter '%s' specified in the request "
                            "URI is not supported." % argname)
                        reraise(exc_type, exc_value, tb.tb_next)
        return kwargs
