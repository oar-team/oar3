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
@app.route('/<any(details, table):details>', methods=['GET'])
@app.route('/nodes/<string:network_address>', methods=['GET'])
@app.route('/ressources/<string:resource_id>/details', methods=['GET'])
@app.args({'offset': Arg(int, default=0),
           'limit': Arg(int),
           'user': Arg(str),
           'from': Arg(int, dest='from_'),
           'to': Arg(int),
           'state': Arg([str, ','], dest='states'),
           'array': Arg(int, dest='array_id'),
           'ids': Arg([int, ':'])})
@app.need_authentication()
def index(offset, limit, user, from_, to, states, array_id, ids, details=None):
    states = states or ['Finishing', 'Running', 'Resuming', 'Suspended',
                        'Launching', 'toLaunch', 'Waiting',
                        'toAckReservation', 'Hold']
    if details:
        query = Job.query
    else:
        query = db.query(Job.id,
                         Job.name,
                         Job.queue_name,
                         Job.user,
                         Job.submission_time)

    page = query.filter_jobs_for_user(user, from_, to, states, array_id, ids)\
                .paginate(offset, limit)
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
