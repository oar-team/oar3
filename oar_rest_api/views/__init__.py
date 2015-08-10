# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

import os
import time
from collections import OrderedDict
from functools import wraps

from flask import Blueprint as FlaskBlueprint, Response, g, abort

from oar.lib import config
from oar.lib.database import BaseModel
from oar.lib.compat import json
from oar.lib.utils import JSONEncoder

from ..utils import ArgParser


class Blueprint(FlaskBlueprint):

    def __init__(self, *args, **kwargs):
        self.root_prefix = kwargs.pop('url_prefix', '')
        self.trailing_slash = kwargs.pop('trailing_slash', True)
        super(Blueprint, self).__init__(*args, **kwargs)
        self.before_request(self._prepare_response)

    def route(self, partial_rule, args={}, jsonify=True, **options):
        """A decorator that is used to define custom routes, injects parsed
        arguments into a view function or method and jsonify the response.

        Example usage with: ::

            @app.route('/factorial', jsonify=True, args={'x': int})
            def factorial(x=0):
                import math
                return {'result': math.factorial(x)}

        """
        rule = partial_rule
        if self.root_prefix:
            rule = (self.root_prefix + partial_rule)
        if self.trailing_slash and len(rule) > 1:
            rule = rule.rstrip("/")

        def decorator(f):
            endpoint = options.pop("endpoint", f.__name__)
            if jsonify and not hasattr(f, "decorated_with_jsonify") and args:
                @self.args(args)
                @self.jsonify
                @wraps(f)
                def wrapper(*proxy_args, **proxy_kwargs):
                    return f(*proxy_args, **proxy_kwargs)
            elif jsonify and not hasattr(f, "decorated_with_jsonify"):
                @self.jsonify
                @wraps(f)
                def wrapper(*proxy_args, **proxy_kwargs):
                    return f(*proxy_args, **proxy_kwargs)
            elif args:
                @self.args(args)
                @wraps(f)
                def wrapper(*proxy_args, **proxy_kwargs):
                    return f(*proxy_args, **proxy_kwargs)
            else:
                wrapper = f
            self.add_url_rule(rule, endpoint, wrapper, **options)
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
                if not isinstance(result, (dict, list, BaseModel)):
                    return result
                return self._jsonify_response(result)
            decorated.decorated_with_jsonify = True
            return decorated
        return decorator

    def args(self, argmap):
        """Decorator that injects parsed arguments into a view function.

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
                parsed_kwargs, raw_kwargs = parser.parse()
                proxy_kwargs.update(parsed_kwargs)
                g.request_args.update(raw_kwargs)
                g.request_args.update(proxy_kwargs)
                return func(*proxy_args, **proxy_kwargs)
            return decorated
        return decorator

    def need_authentication(self):
        """Decorator that check is user is authenticate."""
        def decorator(func):
            @wraps(func)
            def decorated(*proxy_args, **proxy_kwargs):
                if (config.get('API_TRUST_IDENT') or
                        g.current_user is not None):
                    return func(*proxy_args, **proxy_kwargs)
                else:
                    abort(403)
            return decorated
        return decorator

    def _prepare_response(self):
        g.request_args = {}
        g.data = OrderedDict()
        g.data['api_timezone'] = 'UTC'
        g.data['api_timestamp'] = int(time.time())

    def _json_dumps(self, obj, **kwargs):
        """Dump object to json string."""
        kwargs.setdefault('ensure_ascii', False)
        kwargs.setdefault('cls', JSONEncoder)
        kwargs.setdefault('indent', 4)
        kwargs.setdefault('separators', (',', ': '))
        return json.dumps(obj, **kwargs)

    def _jsonify_response(self, obj):
        """Get a json response."""
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
