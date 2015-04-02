# -*- coding: utf-8 -*-
from __future__ import division

from flask import url_for, g
from oar.lib import db
from oar.lib.compat import iteritems

from . import Blueprint


app = Blueprint('resources', __name__, url_prefix="/resources")


def get_links(resource_id, network_address):
    rel_map = {"member": "index", "self": "show", "jobs": "jobs"}
    for rel, endpoint in iteritems(rel_map):
        if rel == "member":
            url = url_for('.%s' % endpoint, network_address=network_address)
        else:
            url = url_for('.%s' % endpoint, resource_id=resource_id)
        yield {'rel': rel, 'href': url, 'title': endpoint}


@app.route('/', methods=['GET'])
@app.route('/nodes/<string:network_address>', methods=['GET'])
@app.args({'offset': int, 'limit': int})
def index(offset=0, limit=None, network_address=None):
    query = db.query(db.m.Resource.id,
                     db.m.Resource.state,
                     db.m.Resource.available_upto,
                     db.m.Resource.network_address)
    if network_address is not None:
        query = query.filter_by(network_address=network_address)
    page = query.paginate(offset, limit)
    g.data['total'] = page.total
    g.data['links'] = [{'rel': 'self', 'href': page.url}]
    if page.has_next:
        g.data['links'].append({'rel': 'next', 'href': page.next_url})
    g.data['offset'] = offset
    g.data['items'] = []
    for item in page:
        item['links'] = list(get_links(item["id"], item["network_address"]))
        g.data['items'].append(item)


@app.route('/<int:resource_id>', methods=['GET'])
def show(resource_id):
    resource = db.m.Resource.query.get_or_404(resource_id)
    g.data.update(resource.asdict())
    g.data['links'] = list(get_links(resource.id, resource.network_address))


@app.route('/<int:resource_id>/jobs', methods=['GET'])
def jobs(resource_id):
    g.data.update(db.m.Resource.query.get_or_404(resource_id).asdict())
