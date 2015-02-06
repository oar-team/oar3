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
        self.before_request(self.prepare_response)

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
        parent_method = super(Blueprint, self).route
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
        g.data['api_timestamp'] = int(time.time())

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
