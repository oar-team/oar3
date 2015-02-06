# -*- coding: utf-8 -*-
from __future__ import division

from flask import url_for, g
from oar.lib import db

from .api import API

api = API('v1', __name__, version='1.0.2')


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


@api.route('/resources', methods=['GET'], args={'offset': int, 'limit': int})
def resources(offset=0, limit=None):
    page = db.query(db.m.Resource.id,
                    db.m.Resource.state,
                    db.m.Resource.available_upto,
                    db.m.Resource.network_address)\
             .paginate(offset, limit)
    g.data['total'] = page.total
    g.data['links'] = [{'rel': 'self', 'href': page.url}]
    if page.has_next:
        g.data['links'].append({'rel': 'next', 'href': page.next_url})
    g.data['offset'] = offset
    g.data['items'] = []
    for item in page:
        item['links'] = []
        item['links'].append({
            'rel': 'self',
            'href': url_for('.resource_overview', resource_id=item['id']),
            'title': 'overview',
        })
        item['links'].append({
            'rel': 'jobs',
            'href': url_for('.resource_jobs', resource_id=item['id']),
            'title': 'jobs',
        })
        g.data['items'].append(item)
    return g.data


@api.route('/resources/<int:resource_id>', methods=['GET'])
def resource_overview(resource_id):
    g.data.update(db.m.Resource.query.get_or_404(resource_id).asdict())
    g.data['links'] = []
    g.data['links'].append({
        'rel': 'self',
        'href': url_for('.resource_overview', resource_id=g.data['id']),
        'title': 'overview',
    })
    g.data['links'].append({
        'rel': 'jobs',
        'href': url_for('.resource_jobs', resource_id=g.data['id']),
        'title': 'jobs',
    })
    return g.data


@api.route('/resources/<int:resource_id>/jobs', methods=['GET'])
def resource_jobs(resource_id):
    g.data.update(db.m.Resource.query.get_or_404(resource_id).asdict())
    return g.data

@api.route('/resources/details')
def full_resources():
    pass


@api.route('/jobs')
def jobs():
    pass


@api.route('/jobs/details')
def detailed_jobs():
    pass


@api.route('/jobs/table')
def jobs_table():
    pass


@api.route('/config')
def config():
    pass


@api.route('/admission_rules')
def admission_rules():
    pass
