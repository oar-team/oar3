# -*- coding: utf-8 -*-
from __future__ import division

from flask import url_for, g, request

from oar.lib import db
from oar.lib.models import Job
from oar.lib.compat import iteritems

from . import Blueprint
from ..utils import Arg


app = Blueprint('jobs', __name__, url_prefix='/jobs')


@app.route('/', methods=['GET'])
@app.route('/<any(details, table):detailed>', methods=['GET'])
@app.route('/nodes/<string:network_address>', methods=['GET'])
@app.route('/ressources/<string:resource_id>/details', methods=['GET'])
@app.args({'offset': Arg(int, default=0),
           'limit': Arg(int),
           'user': Arg(str),
           'from': Arg(int, dest='start_time'),
           'to': Arg(int, dest='stop_time'),
           'state': Arg([str, ','], dest='states'),
           'array': Arg(int, dest='array_id'),
           'ids': Arg([int, ':'], dest='job_ids')})
@app.need_authentication()
def index(offset, limit, user, start_time, stop_time, states, array_id,
          job_ids, detailed=False):
    query = db.queries.get_jobs_for_user(user, start_time, stop_time,
                                         states, array_id, job_ids, detailed)
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
