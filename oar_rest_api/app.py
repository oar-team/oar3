# -*- coding: utf-8 -*-
"""
oar_rest_api.app
~~~~~~~~~~~~~~~~

oar_rest_api application package

"""
from flask import Flask

from oar.lib import db, config
from .query import APIQuery, APIQueryCollection
from .utils import WSGIProxyFix
from .views import register_blueprints
from .errors import register_error_handlers
from .extensions import register_extensions
from .hooks import register_hooks


default_config = {
    "API_TRUST_IDENT": 1,
    "API_DEFAULT_DATA_STRUCTURE": "simple",
    "API_DEFAULT_MAX_ITEMS_NUMBER": 500,
    "API_ABSOLUTE_URIS": 1,
}


def create_app():
    """Return the OAR API application instance."""
    app = Flask(__name__)
    app.wsgi_app = WSGIProxyFix(app.wsgi_app)
    config.setdefault_config(default_config)
    app.config.update(config)
    db.query_class = APIQuery
    db.query_collection_class = APIQueryCollection
    register_error_handlers(app)
    register_hooks(app)
    register_extensions(app)
    register_blueprints(app)
    return app
