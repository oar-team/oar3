# -*- coding: utf-8 -*-
from __future__ import division

import oar.lib
from flask import url_for, g

from . import Blueprint
from .. import API_VERSION, VERSION

app = Blueprint('frontend', __name__)


@app.route('/')
def index():
    g.data['api_version'] = API_VERSION
    g.data['apilib_version'] = VERSION
    g.data['oar_version'] = '2.5.4 (Froggy Summer)'
    g.data['links'] = []
    endpoints = ('resources', 'full_resources', 'jobs',
                 'detailed_jobs', 'jobs_table', 'config', 'admission_rules')
    endpoints = ('resources',)
    for endpoint in endpoints:
        g.data['links'].append({
            'rel': 'collection',
            'href': url_for('%s.index' % endpoint),
            'title': endpoint,
        })


@app.route('/version')
def version():
    g.data['oar_server'] = '3.0.0 (Big Blue)'
    g.data['oar_lib'] = oar.lib.VERSION
    g.data['oar_scheduler'] = 'Kamelot 0.1.0'
    g.data['api'] = API_VERSION
    g.data['api_lib'] = VERSION


@app.route('/whoami')
def whoami():
    g.data['authenticated_user'] = g.current_user


# @api.route('/jobs')
# def jobs():
#     pass


# @api.route('/jobs/details')
# def detailed_jobs():
#     pass


# @api.route('/jobs/table')
# def jobs_table():
#     pass


# @api.route('/config')
# def config():
#     pass


# @api.route('/admission_rules')
# def admission_rules():
#     pass
