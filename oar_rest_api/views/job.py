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
           'state': Arg([str, ','],
                        default=['Finishing', 'Running', 'Resuming',
                                 'Suspended', 'Launching', 'toLaunch',
                                 'Waiting', 'toAckReservation', 'Hold'],
                        dest='states'),
           'from': Arg(int, dest='from_'),
           'to': Arg(int),
           'ids': Arg([int, ':'])})
def index(offset, limit, states, from_, to, ids, details=None):
    pass
    g.data['states'] = states

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
