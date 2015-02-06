# -*- coding: utf-8 -*-
__version__ = '0.1-dev'
VERSION = __version__

from flask import Flask

from oar.lib import db, config
from .query import APIBaseQuery
from .utils import WSGIProxyFix
from .routes import api
from .errors import register_error_handlers
from .hooks import register_hooks


default_config = {
    "API_TRUST_IDENT": 1,
    "API_DEFAULT_DATA_STRUCTURE": "simple",
    "API_DEFAULT_MAX_ITEMS_NUMBER": 500,
    "API_ABSOLUTE_URIS": 1,
}


def create_app():
    app = Flask(__name__)
    app.wsgi_app = WSGIProxyFix(app.wsgi_app)
    config.setdefault_config(default_config)
    app.config.update(config)
    db.query_class = APIBaseQuery
    register_error_handlers(app)
    register_hooks(app)
    app.register_blueprint(api, url_prefix="")
    return app
