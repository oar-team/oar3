

# -*- coding: utf-8 -*-
from __future__ import division

import oar.lib
from flask import url_for, g

from oar import VERSION

from . import Blueprint
from .. import API_VERSION

app = Blueprint('frontend', __name__)


@app.route('/')
def index():
    """ Get all main url section pages. """
    g.data['api_version'] = API_VERSION
    g.data['apilib_version'] = API_VERSION
    g.data['oar_version'] = VERSION
    g.data['links'] = []
    #endpoints = ('resources', 'jobs', 'config', 'admission_rules')
    endpoints = ('resources', 'jobs')
    for endpoint in endpoints:
        g.data['links'].append({
            'rel': 'collection',
            'href': url_for('%s.index' % endpoint),
            'title': endpoint,
        })


@app.route('/version')
def version():
    """Give OAR and OAR API version.
       Also gives the timezone of the API server.
    """
    g.data['oar_server_version'] = VERSION
    g.data['oar_version'] = VERSION
    g.data['oar_lib_version'] = VERSION
    g.data['api_version'] = API_VERSION
    g.data['apilib_version'] = API_VERSION


@app.route('/whoami')
def whoami():
    """Give the name of the authenticated user seen by OAR API.

    The name for a not authenticated user is the null string.
    """
    g.data['authenticated_user'] = g.current_user


@app.route('/timezone')
def timezone():
    """Gives the timezone of the OAR API server. The api_timestamp given in each query is an UTC timestamp (epoch unix time). This timezone information allows you to re-construct the local time. Time is send by defaut (see __init__.py)"""
   
    pass
