# -*- coding: utf-8 -*-
from __future__ import division

from collections import OrderedDict
from flask import url_for, g
from oar.lib import db

from .api import API
from .utils import get_utc_timestamp


api = API('v1', __name__, version='1.0.2')


@api.before_request
def init_global_data():
    g.data = OrderedDict()
    g.data['api_timezone'] ='UTC'
    g.data['api_timestamp'] = get_utc_timestamp()


@api.route('/')
def index():
    g.data['api_version'] = api.version
    g.data['apilib_version'] = api.version
    g.data['oar_version'] = '2.5.4 (Froggy Summer)'
    g.data['links'] = []
    endpoints = ('index', 'resources','full_resources','jobs','detailed_jobs',
                 'jobs_table','config','admission_rules')
    for endpoint in endpoints:
        rel = 'self' if endpoint == 'index' else 'collection'
        g.data['links'].append({
            'rel': rel,
            'href': url_for('.%s' % endpoint),
            'title': endpoint,
        })
    return g.data


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
