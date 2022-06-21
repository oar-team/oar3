# -*- coding: utf-8 -*-
import os

from flask import abort, g, url_for
from passlib.apache import HtpasswdFile

from oar import VERSION
from oar.lib import config

from .. import API_VERSION
from ..utils import Arg
from . import Blueprint

app = Blueprint("frontend", __name__)


@app.route("/")
def index():
    """Get all main url section pages."""
    g.data["api_version"] = API_VERSION
    g.data["apilib_version"] = API_VERSION
    g.data["oar_version"] = VERSION
    g.data["links"] = []
    # endpoints = ('resources', 'jobs', 'config', 'admission_rules')
    endpoints = ("resources", "jobs")
    for endpoint in endpoints:
        g.data["links"].append(
            {
                "rel": "collection",
                "href": url_for("%s.index" % endpoint),
                "title": endpoint,
            }
        )


@app.route("/version")
def version():
    """Give OAR and OAR API version.
    Also gives the timezone of the API server.
    """
    g.data["oar_server_version"] = VERSION
    g.data["oar_version"] = VERSION
    g.data["oar_lib_version"] = VERSION
    g.data["api_version"] = API_VERSION
    g.data["apilib_version"] = API_VERSION


@app.route("/whoami")
def whoami():
    """Give the name of the authenticated user seen by OAR API.

    The name for a not authenticated user is the null string.
    """
    g.data["authenticated_user"] = g.current_user


@app.route("/timezone")
def timezone():
    """Gives the timezone of the OAR API server. The api_timestamp given in each query is an UTC timestamp (epoch unix time). This timezone information allows you to re-construct the local time. Time is send by defaut (see __init__.py)"""
    pass


@app.route("/authentication")
@app.args({"basic_user": Arg(str)})
@app.args({"basic_password": Arg(str)})
def authentication(basic_user, basic_password):
    """allow to test is user/password math htpasswd, can be use as workaround to avoid popup open on browser, usefull for integrated dashboard"""
    htpasswd_filename = None
    if not (basic_user and basic_password):
        abort(400, "Basic authentication is not provided")

    if "HTPASSWD_FILE" in config:
        htpasswd_filename = config["HTPASSWD_FILE"]
    elif os.path.exists("/etc/oar/api-users"):
        htpasswd_filename = "/etc/oar/api-users"
    else:
        abort(404, "File for basic authentication is not found")

    ht = HtpasswdFile(htpasswd_filename)

    if ht.check_password(basic_user, basic_password):
        g.data["basic authentication"] = "valid"
        return

    abort(400, "basic authentication is not validated")
