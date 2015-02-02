# -*- coding: utf-8 -*-
from __future__ import division

from flask import url_for

# from oar.lib import db

from .api import API
from collections import OrderedDict
from .utils import get_utc_timestamp


api = API('v1', __name__, version="1.0.2")


@api.route("/")
def index():
    links = []
    response = OrderedDict()
    response["api_version"] = api.version
    response["apilib_version"] = api.version
    response["api_timezone"] ="UTC"
    response["api_timestamp"] = int(get_utc_timestamp())
    response["oar_version"] = "2.5.4 (Froggy Summer)"
    response["links"] = links
    api_resources = ("", "resources","full_resources","jobs","detailed_jobs",
                     "jobs_table","config","admission_rules")
    for api_resource in api_resources:
        if api_resource:
            links.append({
                'rel': "collection",
                'href': url_for(".%s" % api_resource),
                'title': api_resource,
            })
        else:
            links.append({ 'rel': "self", 'href': url_for(".index"), })
    response["links"] = links
    return response
    # db.models.Job.query.first()


@api.route("/resources")
def resources():
    pass


@api.route("/resources/details")
def full_resources():
    pass


@api.route("/jobs")
def jobs():
    pass


@api.route("/jobs/details")
def detailed_jobs():
    pass


@api.route("/jobs/table")
def jobs_table():
    pass


@api.route("/config")
def config():
    pass


@api.route("/admission_rules")
def admission_rules():
    pass
