# -*- coding: utf-8 -*-
from __future__ import division

from flask import Flask

from . import VERSION
from .utils import WSGIApplication


app = Flask(__name__)
app.wsgi_app = WSGIApplication(app.wsgi_app)


@app.route("/")
def index():
    return "OAR RESTful API version %s" % VERSION
