# -*- coding: utf-8 -*-
from __future__ import division

from flask import url_for, g, request
from oar.lib import db
from oar.lib.models import Resource
from oar.lib.compat import iteritems

from . import Blueprint
from ..utils import Arg


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
@app.route('/details', methods=['GET'])
@app.route('/nodes/<string:network_address>', methods=['GET'])
@app.route('/nodes/<string:network_address>/details', methods=['GET'])
@app.args({'offset': Arg(int, default=0),
           'limit': Arg(int)})
def index(offset, limit, network_address=None):
    if request.path.endswith('/details'):
        query = Resource.query
    else:
        query = db.query(Resource.id,
                         Resource.state,
                         Resource.available_upto,
                         Resource.network_address)

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
    resource = Resource.query.get_or_404(resource_id)
    g.data.update(resource.asdict())
    g.data['links'] = list(get_links(resource.id, resource.network_address))


@app.route('/<int:resource_id>/jobs', methods=['GET'])
def jobs(resource_id):
    g.data.update(Resource.query.get_or_404(resource_id).asdict())
