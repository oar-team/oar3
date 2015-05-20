# -*- coding: utf-8 -*-
from __future__ import division

from flask import url_for, g, request

from oar.lib import db
from oar.lib.models import Resource
from oar.lib.compat import iteritems

from . import Blueprint
from ..utils import Arg


app = Blueprint('jobs', __name__, url_prefix='/jobs')


@app.route('/', methods=['GET'])
@app.route('/<any(details, table):details>', methods=['GET'])
@app.route('/nodes/<string:network_address>', methods=['GET'])
@app.route('/ressources/<string:resource_id>/details', methods=['GET'])
@app.args({'offset': Arg(int, default=0),
           'limit': Arg(int),
           'state': Arg([str, ','], dest='states'),
           'from': Arg(int, dest='from_'),
           'to': Arg(int),
           'ids': Arg([int, ':'])})
@app.need_authentication()
def index(offset, limit, states, from_, to, ids, details=None):
    jobs_query = db.queries.get_jobs_for_user(states, from_, to, ids, details)
    page = jobs_query.paginate(offset, limit)
    g.data['total'] = page.total
    g.data['links'] = [{'rel': 'self', 'href': page.url}]
    if page.has_next:
        g.data['links'].append({'rel': 'next', 'href': page.next_url})
    g.data['offset'] = offset
    g.data['items'] = []
    for item in page:
        # item['links'] = list(get_links(item["id"], item["network_address"]))
        g.data['items'].append(item)


#
# @app.route('/', methods=['GET'])
# @app.args({'offset': int, 'limit': int})
# def index(offset=0, limit=None):
#     pass
#
#
# @app.route('/', methods=['GET'])
# @app.args({'offset': int, 'limit': int})
# def index(offset=0, limit=None):
#     pass
#
#
# @app.route('/', methods=['GET'])
# @app.args({'offset': int, 'limit': int})
# def index(offset=0, limit=None):
#     pass
#
#
# @app.route('/', methods=['GET'])
# @app.args({'offset': int, 'limit': int})
# def index(offset=0, limit=None):
#     pass
#
#
# @app.route('/', methods=['GET'])
# @app.args({'offset': int, 'limit': int})
# def index(offset=0, limit=None):
#     pass
