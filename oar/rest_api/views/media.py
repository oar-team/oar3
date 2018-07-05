# -*- coding: utf-8 -*-
"""
oar.rest_api.views.resource
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Define resources api interaction

"""
import os

from flask import url_for, g, abort, send_from_directory
from . import Blueprint
from ..utils import Arg, list_paginate

import oar.lib.tools as tools

app = Blueprint('media', __name__, url_prefix='/media')

OARDODO_CMD = os.environ['OARDIR'] + '/oardodo/oardodo'

def path_filename_cleaning(path_filename, user):
    # Security escaping
    path_filename = re.sub(r'([$,`, ])',r'\\\1', path_filename)
    
    #$path =~ s/(\\*)(`|\$)/$1$1\\$2/g;

    # Get the path and replace "~" by the home directory
    pw_dir = tools.getpwnam(user).pw_dir
    path_filename = '/' + path_filename
    path_filename.replace('~', pw_dir)

    return path_filename
    
@app.route('/ls/<string:path>', methods=['GET'])
@app.args({'tail': Arg(int)})
@app.args({'offset': Arg(int, default=0),
           'limit': Arg(int)})
@app.need_authentication()
def ls(path=''):
    user = g.current_user
    os.environ['OARDO_BECOME_USER'] = user
    #$ENV{OARDO_BECOME_USER} = $authenticated_user;

    path = path_filename_cleaning(path, user)
    
    # Check directory's existence
    retcode = tools.call('{} test -d {}'.format(OARDODO_CMD, path))
    if retcode:
        abort(404, message='Path not found: {}'.format(path))

    # Check directory's readability
    retcode = tools.call('{} test -r {}'.format(OARDODO_CMD, path))
    if retcode:
        abort(403, message='File could not be read: {}'.format(path))

    # Check if it's a directory
    file_listing = tools.check_output([OARDODO_CMD, 'ls']).decode().split('\n')

    files_with_path = [path + '/' + filename for filename in file_listing[:-1]]
    
    # Get the listing stat -c "%f_%s_%Y_%F_%n"
    ls_results = tools.check_output([OARDODO_CMD, 'stat', '-c', '%f_%s_%Y_%F_%n']
                                    + files_with_path).decode().split('\n')

    file_stats = []
    for i, ls_res in enumerate(ls_results[:-1]):
        f_hex_mode, f_size, f_mtime, f_type = ls_res.split('_')
        file_stats.append({
            name: file_listing[i],
            mode: int(f_hex_mode, 16),
            size: int(f_size),
            mtime: int(f_mtime),
            type: f_type
        })

    list_paginated = list_paginate(file_stats, offset, limit)
        
    g.data['total'] = len(list_paginated)
    url = url_for('%s.%s' % (app.name, endpoint))
    g.data['links'] = [{'rel': 'rel', 'href': url}]
    g.data['offset'] = offset
    g.data['items'] = list_paginate

@app.route('/<string:path_filename>', methods=['GET'])
@app.args({'tail': Arg(int)})
@app.need_authentication()
def get_file(path_filename, tail):
    user = g.current_user
    os.environ['OARDO_BECOME_USER'] = user
    
    path_filename = path_filename_cleaning(path_filename, user)

    # Check file's existence
    retcode = tools.call('{} test -f {}'.format(OARDODO_CMD, path_filename))
    if retcode:
        abort(404, message='File not found: {}'.format(path_filename))

    # Check file's readability
    retcode = tools.call('{} test -r {}'.format(OARDODO_CMD, path_filename))
    if retcode:
        abort(403, message='File could not be read: {}'.format(path_filename))

    file_content = None
    if tail:        
        file_content = check_output([OARDODO_CMD, 'cat', path_filename])
    else:
        file_content = check_output([OARDODO_CMD, 'tail', '-n', str(tail), path_filename])

    return Response(file_content, mimetype='application/octet-stream')

def upload_file():
    pass

def chmod():
    pass




# Post
#elif request.headers['Content-Type'] == 'application/octet-stream':
#        f = open('./binary', 'wb')
#        f.write(request.data)
#                f.close()
#        return "Binary message written!"
