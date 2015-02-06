# -*- coding: utf-8 -*-
from __future__ import division

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


# @api.route('/resources/details')
# def full_resources():
#     pass


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
