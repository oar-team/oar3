# -*- coding: utf-8 -*-
"""
oar.rest_api.views.resource
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Define resources api interaction

"""
import os

from flask import abort
from . import Blueprint
from ..utils import Arg

import oar.lib.tools as tools

app = Blueprint('media', __name__, url_prefix='/media')

OARDODO_CMD = os.environ['OARDIR'] + '/oardodo/oardodo'

@app.route('/ls/<string:path>', methods=['GET'])
@app.args({'tail': Arg(int)})
@app.need_authentication()
def ls(path=''):
    user = g.current_user
    os.environ['OARDO_BECOME_USER'] = user
    #$ENV{OARDO_BECOME_USER} = $authenticated_user;

    # Security escaping
    path = re.sub(r'([$,`, ])',r'\\\1', path)
    
    #$path =~ s/(\\*)(`|\$)/$1$1\\$2/g;

    # Get the path and replace "~" by the home directory
    pw_dir = tools.getpwnam(user).pw_dir
    path = '/' + path
    path.replace('~', pw_dir)
     
    # Check file existency
    retcode = tools.call('{} test -d {}'.format(OARDODO_CMD, path))
    if retcode:
        abort(404)

    # Check file readability
    retcode = tools.call('{} test -r {}'.format(OARDODO_CMD, path))
    if retcode:
        abort(403)

    # Get the listing
    ls_results = tools.check_output([OARDODO_CMD, 'ls', '-l', path]).split('\n')

    for ls_res in ls_results:
        if ls_res:
            l_name = ls_rs[5]
            l_type = ls_rs[4]
            l_size = ls_rs[3]
            l_mtime = ls_rs[0]
            l_mode = ls_rs[0]
    
def chmod():
    pass

