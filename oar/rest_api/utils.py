# -*- coding: utf-8 -*-
import os
import sys

from flask import abort, current_app, request

from oar.lib import config
from oar.lib.utils import integer_types, reraise, to_unicode


class WSGIProxyFix(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        user = os.environ.get("AUTHENTICATE_UID", None)
        if user is not None:
            environ["USER"] = user
        else:
            if config.get("API_TRUST_IDENT", 0) == 1:
                user = environ.pop("HTTP_X_REMOTE_IDENT", None)
                if user not in ("", "unknown", "(null)"):
                    environ["USER"] = user
        return self.app(environ, start_response)


class PrefixMiddleware(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        prefix = environ.pop("HTTP_X_API_PATH_PREFIX", None)
        if prefix is not None:
            environ["SCRIPT_NAME"] = prefix
        return self.app(environ, start_response)


class Arg(object):
    """Request argument type."""

    DEFAULT_LOCATIONS = ("querystring", "form", "json")

    def __init__(
        self,
        type_=None,
        default=None,
        required=False,
        error=None,
        locations=None,
        dest=None,
    ):
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
        if isinstance(self.type, ListArg) and default is None:
            self.default = []
        else:
            self.default = default
        self.required = required
        self.dest = dest
        self.error = error
        self.locations = locations or self.DEFAULT_LOCATIONS

    def raw_value(self, value):
        if value is not None:
            if isinstance(self.type, ListArg):
                if len(value) > 0:
                    return self.type.raw_value(value)
            else:
                return to_unicode(value)


class ListArg(object):
    def __init__(self, type_=str, sep=None):
        self.type = type_
        self.sep = sep

    def __call__(self, value, callback):
        if not self.sep:  # TOFINISH
            return value

        def convert():
            string = to_unicode(value)
            if string:
                for item in string.split(self.sep):
                    yield callback(item, self.type)

        return list(convert())

    def raw_value(self, values):
        sep = self.sep if self.sep else ", "
        return to_unicode(sep.join(("%s" % v for v in values)))


class ArgParser(object):
    """Flask request argument parser."""

    MISSING = object()

    def __init__(self, argmap):
        self.argmap = argmap

    def get_value(self, data, name, argobj):
        if isinstance(argobj.type, ListArg) and not argobj.type.sep:
            return data.getlist(name)
        else:
            return data.get(name, self.MISSING)

    def parse_arg(self, argname, argobj):
        """Pull a form value from the request."""
        for location in argobj.locations:
            value = self.MISSING
            if location == "querystring" and request.args:
                value = self.get_value(request.args, argname, argobj)
            elif location == "json":
                json_data = request.get_json(silent=True, force=True)
                if json_data:
                    value = self.get_value(json_data, argname, argobj)
                else:
                    value = self.MISSING
            elif location == "form":
                value = self.get_value(request.form, argname, argobj)
            if value is not self.MISSING:
                return value
        return self.MISSING

    def convert_bool(self, value):
        """Try to convert ``value`` to a Boolean."""
        if value.lower() in ("True", "yes", "1"):
            return True
        if value.lower() in ("false", "no", "0"):
            return False
        raise ValueError("Cannot convert '%s' to a Boolean value" % value)

    def convert_int(self, value):
        """Try to convert ``value`` to an Integer."""
        try:
            value = float(value)
        except Exception:
            pass
        for _type in integer_types:
            try:
                return _type(value)
            except Exception:
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
        parsed_kwargs = {}
        raw_kwargs = {}
        for argname, argobj in self.argmap.items():
            dest = argobj.dest if argobj.dest is not None else argname
            parsed_value = self.parse_arg(argname, argobj)
            if parsed_value is not self.MISSING:
                try:
                    parsed_kwargs[dest] = self.convert(parsed_value, argobj.type)
                except Exception as e:
                    msg = (
                        "The parameter '%s' specified in the request "
                        "URI is not supported. %s" % (argname, e)
                    )
                    try:
                        abort(400)
                    except Exception:
                        exc_type, exc_value, tb = sys.exc_info()
                        exc_value.data = msg
                        reraise(exc_type, exc_value, tb.tb_next)
            else:
                parsed_kwargs[dest] = argobj.default
            raw_value = argobj.raw_value(parsed_kwargs[dest])
            if raw_value is not None:
                raw_kwargs[argname] = raw_value
        return parsed_kwargs, raw_kwargs


def list_paginate(items, offset, limit, error_out=True):
    if error_out and (offset < 0 or offset > len(items)):
        abort(404)

    if limit is None:
        limit = current_app.config.get("API_DEFAULT_MAX_ITEMS_NUMBER")

    items_paginated = items[offset : min(len(items), offset + limit)]

    return items_paginated
