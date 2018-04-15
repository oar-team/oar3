# -*- coding: utf-8 -*-
"""
oar.rest_api.app
~~~~~~~~~~~~~~~~

OAR rest_api application package

"""
from flask import Flask

from oar.lib import db, config
from .query import APIQuery, APIQueryCollection
from .utils import WSGIProxyFix, PrefixMiddleware
from .views import register_blueprints
from .errors import register_error_handlers
from .extensions import register_extensions
from .hooks import register_hooks
from .proxy import register_proxy


default_config = {
    "API_TRUST_IDENT": 1,
    "API_DEFAULT_DATA_STRUCTURE": "simple",
    "API_DEFAULT_MAX_ITEMS_NUMBER": 500,
    "API_ABSOLUTE_URIS": 1,
}


def create_app(**kwargs):
    """Return the OAR API application instance."""
    app = Flask(__name__)
    app.wsgi_app = WSGIProxyFix(app.wsgi_app)
    app.wsgi_app = PrefixMiddleware(app.wsgi_app)
    config.setdefault_config(default_config)
    app.config.update(config)
    db.query_class = APIQuery
    db.query_collection_class = APIQueryCollection
    register_error_handlers(app)
    register_hooks(app)
    register_extensions(app)
    register_blueprints(app)
    register_proxy(app, **kwargs)
    
    return app
    
def wsgi_app(environ, start_response):
    # For use within WSGI context (for instance mod_wsgi for apache2)
    # cat /usr/local/lib/cgi-bin/oarapi/oarapi.wsgi
    # from oar.rest_api.app import wswgi_app as application
    app = create_app()
    return app(environ, start_response)
