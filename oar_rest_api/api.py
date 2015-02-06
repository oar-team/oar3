# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

import sys
import json
from collections import OrderedDict
from functools import wraps

from flask import Blueprint, Response, abort, request, g
from oar.lib.compat import reraise, to_unicode, iteritems
from oar.lib.database import BaseModel

from .utils import JSONEncoder, get_utc_timestamp


class API(Blueprint):

    def __init__(self, *args, **kwargs):
        version = kwargs.pop("version", None)
        super(API, self).__init__(*args, **kwargs)
        self.version = version if version is not None else self.name
        self.before_request(self.prepare_response)


    def route(self, rule, args={}, **options):
        """A decorator that is used to define custom routes, injects parsed
        arguments into a view function or method and jsonify the response.

        Example usage with: ::

            @api.route('/hello', methods=['get', 'post'], args={'name': str})
            def greet(name="world"):
                return {'message': 'Hello ' + name}

        """
        parent_method = super(API, self).route
        def decorator(f):
            @wraps(f)
            def decorated(*proxy_args, **proxy_kwargs):
                if args:
                    parser = ArgParser(args)
                    parsed_kwargs = parser.parse()
                    proxy_kwargs.update(parsed_kwargs)
                result = f(*proxy_args, **proxy_kwargs)
                if result is None:
                    result = {}
                if isinstance(result, (dict, list, BaseModel)):
                    return self._json_response(result)
                return result
            parent_method(rule, **options)(decorated)
            parent_method(rule + ".json", **options)(decorated)
        return decorator

    def prepare_response(self):
        g.data = OrderedDict()
        g.data['api_timezone'] ='UTC'
        g.data['api_timestamp'] = get_utc_timestamp()

    def _json_dumps(self, obj, **kwargs):
        """Dumps object to json string. """
        kwargs.setdefault('ensure_ascii', False)
        kwargs.setdefault('cls', JSONEncoder)
        kwargs.setdefault('indent', 4)
        kwargs.setdefault('separators', (',', ': '))
        kwargs.setdefault('encoding', 'utf-8')
        return json.dumps(obj, **kwargs)

    def _json_response(self, obj):
        """Get a json response. """
        return Response(self._json_dumps(obj),
                        mimetype='application/json')


class ArgParser(object):
    """Flask request argument parser."""

    DEFAULT_TARGETS = ('querystring', 'json', 'form')

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
                json_data = request.get_json(silent=True)
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
