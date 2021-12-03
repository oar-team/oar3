# -*- coding: utf-8 -*-
from flask import jsonify
from werkzeug.exceptions import HTTPException, default_exceptions

from oar.lib.utils import to_unicode


def register_error_handlers(app):
    """Creates a JSON-oriented Flask app.

    All error responses that you don't specifically
    manage yourself will have application/json content
    type, and will contain JSON like this (just an example):

    { "code": 405, "message": "405: Method Not Allowed" }
    """

    def make_json_error(ex):
        code = ex.code if isinstance(ex, HTTPException) else 500
        message = to_unicode(ex)
        data = getattr(ex, "data", None)
        if data:
            message = to_unicode(data)
        response = jsonify(code=code, message=message)
        response.status_code = code
        return response

    for code in default_exceptions.keys():
        app.errorhandler(code)(make_json_error)

    return app
