# -*- coding: utf-8 -*-

"""
oar.rest_api.views.stress_factor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Define stress_factor retreiving api

"""
import os
from flask import (url_for, g, abort)

from . import Blueprint
from ..utils import Arg

from oar.lib import config

import oar.lib.tools as tools

app = Blueprint('stress_factor', __name__, url_prefix='/stress_factor')


if 'OARDIR' not in os.environ:
    os.environ['OARDIR'] = '/usr/local/lib/oar'
     
OARDODO_CMD = os.environ['OARDIR'] + '/oardodo/oardodo'

@app.route('/', methods=['GET'])
def index():
    stress_factor_script = '/etc/oar/stress_factor.sh'
    if 'API_STRESS_FACTOR_SCRIPT' in config:
        stress_factor_script = config['API_STRESS_FACTOR_SCRIPT']

    cmd = [OARDODO_CMD, 'bash', '--noprofile', '--norc', '-c', 'stress_factor_script']

    stress_factor_result = tools.check_output(cmd).decode().split('\n')

    global_stress = None
    stress_factor_value = ''
    
    for sf_res in stress_factor_result:
        sf = sf_res.split('=')
        if (len(sf) == 2) and (sf[0] == 'GLOBAL_STRESS'):
            global_stress = sf[0]
            stress_factor_value = sf[1]
    if global_stress:
        g.data[global_stress] = stress_factor_value
        url = url_for('%s.index' % app.name)
        g.data['links'] = [{'rel': 'rel', 'href': url}] 

    else:
        abort(404, 'Unable to retrieve STRESS_FACTOR')
