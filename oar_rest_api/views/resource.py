# -*- coding: utf-8 -*-
from __future__ import division

from flask import url_for, g
from oar.lib import db
from oar.lib.models import Resource

from . import Blueprint
from ..utils import Arg


app = Blueprint('resources', __name__, url_prefix="/resources")


@app.route('/', methods=['GET'])
@app.route('/<any(details, full):detailed>', methods=['GET'])
@app.route('/nodes/<string:network_address>', methods=['GET'])
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
        attach_links(item)
        g.data['items'].append(item)


@app.route('/<int:resource_id>', methods=['GET'])
def show(resource_id):
    resource = Resource.query.get_or_404(resource_id)
    g.data.update(resource.asdict())
    attach_links(g.data)


@app.route('/<int:resource_id>/jobs', methods=['GET'])
def jobs(resource_id):
    g.data.update(Resource.query.get_or_404(resource_id).asdict())


def attach_links(resource):
    rel_map = (
        ("node", "member", "index"),
        ("show", "self", "show"),
        ("jobs", "collection", "jobs"),
    )
    links = []
    for title, rel, endpoint in rel_map:
        if title == "node" and "network_address" in resource:
            url = url_for('%s.%s' % (app.name, endpoint),
                          network_address=resource['network_address'])
            links.append({'rel': rel, 'href': url, 'title': title})
        elif title != "node" and "id" in resource:
            url = url_for('%s.%s' % (app.name, endpoint),
                          resource_id=resource['id'])
            links.append({'rel': rel, 'href': url, 'title': title})
    resource['links'] = links
