# -*- coding: utf-8 -*-
from __future__ import division

import sys
import json
import decimal
import datetime

from flask import request, abort
from oar.lib.compat import reraise, to_unicode, iteritems, integer_types


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
    """Request argument type."""

    DEFAULT_LOCATIONS = ('querystring', 'form', 'json')

    def __init__(self, type_=None, default=None, required=False,
                 error=None, locations=None, dest=None):
        if isinstance(type_, (tuple, list)):
            if len(type_) >= 2:
                self.type = ListArg(type_[0], type_[1])
            elif len(type_):
                self.type = ListArg(type_[0])
            else:
                self.type = ListArg()
        elif type_ == list:
            self.type = ListArg()
        elif type_ is None:
            self.type = lambda x: x  # default to no type conversion
        else:
            self.type = type_
        self.default = default
        self.required = required
        self.dest = dest
        self.error = error
        self.locations = locations or self.DEFAULT_LOCATIONS


class ListArg(object):
    def __init__(self, type_=str, sep=":"):
        self.type = type_
        self.sep = sep

    def __call__(self, value, callback):
        def convert():
            string = to_unicode(value)
            if string:
                for item in string.split(self.sep):
                    yield callback(item, self.type)
        return list(convert())


class ArgParser(object):
    """Flask request argument parser."""

    MISSING = object()

    def __init__(self, argmap):
        self.argmap = argmap

    def get_value(self, data, name):
        return data.get(name, self.MISSING)

    def parse_arg(self, argname, argobj):
        """Pull a form value from the request."""
        for location in argobj.locations:
            if location == "querystring":
                value = self.get_value(request.args, argname)
            elif location == "json":
                json_data = request.get_json(silent=True, force=True)
                if json_data:
                    value = self.get_value(json_data, argname)
                else:
                    value = self.MISSING
            elif location == "form":
                value = self.get_value(request.form, argname)
            if value is not self.MISSING:
                return value
        return self.MISSING

    def convert_bool(self, value):
        """ Try to convert ``value`` to a Boolean."""
        if value.lower() in ('True', 'yes', '1'):
            return True
        if value.lower() in ('false', 'no', '0'):
            return False
        raise ValueError("Cannot convert '%s' to a Boolean value" % value)

    def convert_int(self, value):
        """ Try to convert ``value`` to an Integer."""
        try:
            value = float(value)
        except:
            pass
        for _type in integer_types:
            try:
                return _type(value)
            except:
                pass
        raise ValueError("Cannot convert '%s' to a Integer value" % value)

    def convert(self, value, argtype):
        if argtype == str:
            return to_unicode(value)
        elif argtype == bool:
            return self.convert_bool(value)
        elif argtype in integer_types:
            return self.convert_int(value)
        if isinstance(argtype, ListArg):
            return argtype(value, self.convert)
        else:
            return argtype(value)

    def parse(self):
        """Parses the request arguments."""
        kwargs = {}
        for argname, argobj in iteritems(self.argmap):
            dest = argobj.dest if argobj.dest is not None else argname
            parsed_value = self.parse_arg(argname, argobj)
            if parsed_value is not self.MISSING:
                try:
                    kwargs[dest] = self.convert(parsed_value, argobj.type)
                except Exception as e:
                    msg = ("The parameter '%s' specified in the request "
                           "URI is not supported. %s" % (argname, e))
                    try:
                        abort(400)
                    except:
                        exc_type, exc_value, tb = sys.exc_info()
                        exc_value.data = msg
                        reraise(exc_type, exc_value, tb.tb_next)
            else:
                kwargs[dest] = argobj.default
        return kwargs
