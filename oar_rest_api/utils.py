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
        user = environ.pop('HTTP_X_REMOTE_IDENT', None)
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
            return to_unicode(obj)
        elif hasattr(obj, '_asdict') and callable(getattr(obj, '_asdict')):
            return obj._asdict()
        elif hasattr(obj, 'asdict') and callable(getattr(obj, 'asdict')):
            return obj.asdict()
        else:
            return json.JSONEncoder.default(self, obj)


class Arg(object):
    """A request argument."""

    DEFAULT_LOCATIONS = ('querystring', 'form', 'json')

    def __init__(self, type_=None, default=None, required=False,
                 multiple=False, error=None, locations=None):
        if type_:
            self.type = type_
        else:
            self.type = lambda x: x  # default to no type conversion
        if multiple and default is None:
            self.default = []
        else:
            self.default = default
        self.required = required
        self.error = error
        self.multiple = multiple
        self.locations = locations or self.DEFAULT_LOCATIONS


class ListArg(object):
    def __init__(self, type_=str, sep=":"):
        self.type = type_
        self.sep = sep

    def __call__(self, value, callback):
        def convert():
            string = to_unicode(value)
            for item in string.split(self.sep):
                yield callback(item, self.type)
        return list(convert())


class ArgParser(object):
    """Flask request argument parser."""

    MISSING = object()

    def __init__(self, argmap):
        self.argmap = argmap

    def get_value(self, data, name, multiple):
        value = data.get(name, self.MISSING)
        if multiple and value is not self.MISSING:
            if hasattr(data, 'getlist'):
                return data.getlist(name)
            elif hasattr(data, 'getall'):
                return data.getall(name)
            elif isinstance(value, (list, tuple)):
                return value
            else:
                return [value]
        return value

    def parse_arg(self, argname, argobj):
        """Pull a form value from the request."""
        for location in argobj.locations:
            if location == "querystring":
                value = self.get_value(request.args, argname, argobj.multiple)
            elif location == "json":
                json_data = request.get_json(silent=True, force=True)
                if json_data:
                    value = self.get_value(json_data, argname, argobj.multiple)
                else:
                    value = self.MISSING
            elif location == "form":
                value = self.get_value(request.form, argname, argobj.multiple)
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
        for argname, argobj in iteritems(self.argmap):
            parsed_value = self.parse_arg(argname, argobj)
            if parsed_value is not self.MISSING:
                try:
                    kwargs[argname] = self.convert(parsed_value, argobj)
                except:
                    try:
                        abort(400)
                    except:
                        exc_type, exc_value, tb = sys.exc_info()
                        exc_value.data = \
                            ("The parameter '%s' specified in the request "
                             "URI is not supported." % argname)
                        reraise(exc_type, exc_value, tb.tb_next)
            else:
                kwargs[argname] = argobj.default
        return kwargs
