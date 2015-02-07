# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

import os
import json
import time
from collections import OrderedDict
from functools import wraps

from flask import Blueprint as FlaskBlueprint, Response, g

from oar.lib.database import BaseModel

from ..utils import JSONEncoder, ArgParser


class Blueprint(FlaskBlueprint):

    def __init__(self, *args, **kwargs):
        self.root_prefix = kwargs.pop('url_prefix', '')
        self.trailing_slash = kwargs.pop('trailing_slash', True)
        super(Blueprint, self).__init__(*args, **kwargs)
        self.before_request(self._prepare_response)

    def route(self, partial_rule, args={}, **options):
        """A decorator that is used to define custom routes, injects parsed
        arguments into a view function or method and jsonify the response.

        Example usage with: ::

            @app.route('/factorial', methods=['GET', 'POST'], args={'x': int})
            def factorial(x=0):
                import math
                return {'result': math.factorial(x)}
        """
        if self.root_prefix:
            rule = self.root_prefix + partial_rule
        else:
            rule = partial_rule
        if self.trailing_slash and len(rule) > 1:
            rule = rule.rstrip("/")

        def decorator(f):
            @self.jsonify
            @self.args(args)
            def wrapper(*proxy_args, **proxy_kwargs):
                return f(*proxy_args, **proxy_kwargs)
            endpoint = options.pop("endpoint", f.__name__)
            self.add_url_rule(rule, endpoint, wrapper, **options)
            self.add_url_rule(rule + ".json", endpoint + "_json", wrapper,
                              **options)
            return wrapper
        return decorator

    @property
    def jsonify(self):
        """A decorator that is used to jsonify the response.

        Example usage with: ::

            @app.jsonify('/foo')
            def foo(name="bar"):
                g.data["foo"] = name  # or return {"foo": name}
        """
        def decorator(func):
            @wraps(func)
            def decorated(*proxy_args, **proxy_kwargs):
                result = func(*proxy_args, **proxy_kwargs)
                if result is None:
                    result = g.data
                if isinstance(result, (dict, list, BaseModel)):
                    return self._jsonify_response(result)
                return result
            return decorated
        return decorator

    def args(self, argmap):
        """Decorator that injects parsed arguments into a view function or
        method.

        Example usage with: ::

            @app.route('/factorial', methods=['GET', 'POST'])
            @app.args({'x': int})
            def factorial(x=0):
                import math
                return {'result': math.factorial(x)}
        """
        def decorator(func):
            @wraps(func)
            def decorated(*proxy_args, **proxy_kwargs):
                parser = ArgParser(argmap)
                parsed_kwargs = parser.parse()
                proxy_kwargs.update(parsed_kwargs)
                g.request_params.update(proxy_kwargs)
                return func(*proxy_args, **proxy_kwargs)
            return decorated
        return decorator

    def _prepare_response(self):
        g.request_params = {}
        g.data = OrderedDict()
        g.data['api_timezone'] ='UTC'
        g.data['api_timestamp'] = int(time.time())

    def _json_dumps(self, obj, **kwargs):
        """Dumps object to json string. """
        kwargs.setdefault('ensure_ascii', False)
        kwargs.setdefault('cls', JSONEncoder)
        kwargs.setdefault('indent', 4)
        kwargs.setdefault('separators', (',', ': '))
        kwargs.setdefault('encoding', 'utf-8')
        return json.dumps(obj, **kwargs)

    def _jsonify_response(self, obj):
        """Get a json response. """
        return Response(self._json_dumps(obj), mimetype='application/json')


def load_blueprints():
    folder = os.path.abspath(os.path.dirname(__file__))
    for filename in os.listdir(folder):
        if filename.endswith('.py') and not filename.startswith('__'):
            name = filename[:-3]
            module_path = 'oar.rest_api.views.%s' % name
            module = __import__(module_path, None, None, ['app'])
            yield getattr(module, 'app')


def register_blueprints(app):
    for blueprint in load_blueprints():
        app.register_blueprint(blueprint)
