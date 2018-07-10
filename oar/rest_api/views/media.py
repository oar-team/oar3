# -*- coding: utf-8 -*-
"""
oar.rest_api.views.resource
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Define resources api interaction

"""
import os
import re

from flask import (url_for, g, abort, send_from_directory, make_response,
                   Response, request)
from . import Blueprint
from ..utils import Arg, list_paginate

import oar.lib.tools as tools

app = Blueprint('media', __name__, url_prefix='/media')

if 'OARDIR' not in os.environ:
    os.environ['OARDIR'] = '/usr/local/lib/oar'
     
OARDODO_CMD = os.environ['OARDIR'] + '/oardodo/oardodo'

def user_and_filename_setup(path_filename):
    # user setup
    user = g.current_user
    os.environ['OARDO_BECOME_USER'] = user

    # Security escaping
    path_filename = re.sub(r'([$,`, ])', r'\\\1', path_filename)
    
    #$path =~ s/(\\*)(`|\$)/$1$1\\$2/g;

    # Get the path and replace "~" by the home directory
    pw_dir = tools.getpwnam(user).pw_dir
    path_filename = '/' + path_filename
    path_filename.replace('~', pw_dir)

    return path_filename
    
@app.route('/ls/<string:path>', methods=['GET'])
@app.args({'offset': Arg(int, default=0), 'limit': Arg(int)})
@app.need_authentication()
def ls(offset, limit, path='~'):
    #import pdb; pdb.set_trace()
    path = user_and_filename_setup(path)
    
    # Check directory's existence
    retcode = tools.call('{} test -d {}'.format(OARDODO_CMD, path))
    if retcode:
        abort(404, 'Path not found: {}'.format(path))

    # Check directory's readability
    retcode = tools.call('{} test -r {}'.format(OARDODO_CMD, path))
    if retcode:
        abort(403, 'File could not be read: {}'.format(path))

    # Check if it's a directory
    file_listing = tools.check_output([OARDODO_CMD, 'ls']).decode().split('\n')

    files_with_path = [path + '/' + filename for filename in file_listing[:-1]]
    
    # Get the listing stat -c "%f_%s_%Y_%F_%n"
    ls_results = tools.check_output([OARDODO_CMD, 'stat', '-c', '%f_%s_%Y_%F']
                                    + files_with_path).decode().split('\n')

    file_stats = []
    for i, ls_res in enumerate(ls_results[:-1]):
        f_hex_mode, f_size, f_mtime, f_type = ls_res.split('_')
        file_stats.append({
            'name': file_listing[i],
            'mode': int(f_hex_mode, 16),
            'size': int(f_size),
            'mtime': int(f_mtime),
            'type': f_type
        })

    list_paginated = list_paginate(file_stats, offset, limit)
        
    g.data['total'] = len(list_paginated)
    url = url_for('%s.%s' % (app.name, 'ls'), path=path)
    g.data['links'] = [{'rel': 'rel', 'href': url}]
    g.data['offset'] = offset
    g.data['items'] = list_paginated

@app.route('/<path:path_filename>', methods=['GET'])
@app.args({'tail': Arg(int)})
@app.need_authentication()
def get_file(path_filename, tail):

    path_filename = user_and_filename_setup(path_filename)

    # Check file's existence
    retcode = tools.call('{} test -f {}'.format(OARDODO_CMD, path_filename))
    if retcode:
        abort(404, 'File not found: {}'.format(path_filename))

    # Check file's readability
    retcode = tools.call('{} test -r {}'.format(OARDODO_CMD, path_filename))
    if retcode:
        abort(403, 'File could not be read: {}'.format(path_filename))

    file_content = None
    if tail:        
        file_content = tools.check_output([OARDODO_CMD, 'tail', '-n', str(tail), path_filename])
    else:
        file_content = tools.check_output([OARDODO_CMD, 'cat', path_filename])
    return Response(file_content, mimetype='application/octet-stream')

@app.route('/<path:path_filename>', methods=['POST', 'PUT'])
@app.args({ 'force': Arg(bool, default=0)})
@app.need_authentication()
def post_file(path_filename, force):
    path_filename = user_and_filename_setup(path_filename)
    # Check file's existence
    if not force:
        retcode = tools.call('{} test -f {}'.format(OARDODO_CMD, path_filename))
        if not retcode:
            abort(403, 'The file already exists: {}'.format(path_filename))

    cmd = [OARDODO_CMD, 'bash', '--noprofile', '--norc', '-c', 'cat > ' + path_filename]

    if request.headers['Content-Type'] == 'application/octet-stream':
        p = tools.Popen(cmd, stdin=PIPE)
        try: 
            p.communicate(request.data)
        except Exception as ex:
            p.kill()
            abort(501, ex)
    else:
        if 'file' not in request.files:
            abort(400, 'No file part')
        file = request.files['file']    
        if file.filename == '':
            abort(400, 'No selected file')
        try: 
            p = tools.Popen(cmd, stdin=file)
        except Exception as ex:
            p.kill()
            abort(501, ex)
            
    #url = url_for('%s.post_file' % app.name, path_filename=path_filename[1:])
    url = app.name + path_filename
    g.data['links'] = [{'rel': 'rel', 'href': url}]
    g.data['status'] = 'created'
    g.data['success'] = 'true'
    
@app.route('/<string:path_filename>', methods=['DELETE'])
@app.need_authentication()
def delete(path_filename):
    path_filename = user_and_filename_setup(path_filename)

    # Check file's existence
    retcode = tools.call('{} test -e {}'.format(OARDODO_CMD, path_filename))
    if retcode:
        abort(404, 'File not found: {}'.format(path_filename))

    # Check file readability
    retcode = tools.call('{} test -w {}'.format(OARDODO_CMD, path_filename))
    if retcode:
        abort(403, 'File or directory is not writeable: {}'.format(path_filename))
        
    # Delete the file
    retcode = tools.call('{} rm -rf {}'.format(OARDODO_CMD, path_filename))
    if retcode:
        abort(501, 'File unkown error, rm -rf failed for : {}'.format(path_filename))

    response = make_response('', 204)
    response.mimetype = 'application/octet-stream'
    return response

@app.route('/chmod/<string:path_filename>', methods=['POST'])
@app.args({'mode': Arg(str)})
@app.need_authentication()
def chmod():
    path_filename = user_and_filename_setup(path_filename)
    # Check file's existence
    retcode = tools.call('{} test -e {}'.format(OARDODO_CMD, path_filename))
    if retcode:
        abort(404, 'File not found: {}'.format(path_filename))
        
    # Security checking
    if not mode.isalnum():
        abort(402, 'Bad mode value: {}'.format(mode)) 

    # Do the chmod
    retcode = tools.call('{} chmod {} {}'.format(OARDODO_CMD, mode, path_filename))
    if retcode:
        abort(500, 'Could not set mode {} on file {}'.format(mode, path_filename))

    response = make_response('', 202)
    response.mimetype = 'application/octet-stream'
    return response
