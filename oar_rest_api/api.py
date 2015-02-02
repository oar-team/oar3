# -*- coding: utf-8 -*-
from functools import wraps

import json

from flask import Blueprint, Response

from oar.lib import db

from .utils import JSONEncoder


class API(Blueprint):

    def __init__(self, *args, **kwargs):
        version = kwargs.pop("version", None)
        super(API, self).__init__(*args, **kwargs)
        self.version = version if version is not None else self.name

    def route(self, rule, **options):
        """A decorator that is used to define custom routes for methods in
        FlaskView subclasses. The format is exactly the same as Flask's
        `@app.route` decorator.
        """
        parent_method = super(API, self).route

        def decorator(f):
            @wraps(f)
            def decorated(*args, **kwargs):
                result = f(*args, **kwargs)
                if result is None:
                    result = {}
                if isinstance(result, (dict, list, db.Model)):
                    return self._json_response(result)
                return result
            parent_method(rule, **options)(decorated)
            parent_method(rule + ".json", **options)(decorated)
        return decorator

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
