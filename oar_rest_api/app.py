# -*- coding: utf-8 -*-
__version__ = '0.1-dev'
VERSION = __version__

from flask import Flask

from oar.lib import db, config
from .api import APIBaseModel, APIBaseQuery
from .utils import WSGIProxyFix
from .routes import api
from .exceptions import configure_errorhandlers


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
    db.init_flask_app(app)
    configure_errorhandlers(app)
    db.query_class = APIBaseQuery
    db.model_class = APIBaseModel
    app.register_blueprint(api, url_prefix="")
    return app
