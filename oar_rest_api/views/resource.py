# -*- coding: utf-8 -*-
from __future__ import division

from flask import url_for, g
from oar.lib import db
from oar.lib.models import Resource

from . import Blueprint
from ..utils import Arg


app = Blueprint('resources', __name__, url_prefix="/resources")


def get_items_links(resource_id, network_address):
    rel_map = (
        ("node", "member", "index"),
        ("show", "self", "show"),
        ("jobs", "collection", "jobs"),
    )
    links = []
    for title, rel, endpoint in rel_map:
        if title == "node":
            url = url_for('.%s' % endpoint, network_address=network_address)
        else:
            url = url_for('.%s' % endpoint, resource_id=resource_id)
        links.append({'rel': rel, 'href': url, 'title': title})
    return links


@app.route('/', methods=['GET'])
@app.route('/<any(details, full):detailed>', methods=['GET'])
@app.route('/details', methods=['GET'])
@app.route('/nodes/<string:network_address>', methods=['GET'])
@app.route('/nodes/<string:network_address>/details', methods=['GET'])
@app.args({'offset': Arg(int, default=0),
           'limit': Arg(int)})
def index(offset, limit, network_address=None, detailed=False):
    query = db.queries.get_resources(network_address, detailed)
    page = query.paginate(offset, limit)
    g.data['total'] = page.total
    g.data['links'] = page.links
    g.data['offset'] = offset
    g.data['items'] = []
    for item in page:
        item['links'] = get_items_links(item["id"], item["network_address"])
        g.data['items'].append(item)


@app.route('/<int:resource_id>', methods=['GET'])
def show(resource_id):
    resource = Resource.query.get_or_404(resource_id)
    g.data.update(resource.asdict())
    g.data['links'] = get_items_links(resource.id, resource.network_address)


@app.route('/<int:resource_id>/jobs', methods=['GET'])
def jobs(resource_id):
    g.data.update(Resource.query.get_or_404(resource_id).asdict())
