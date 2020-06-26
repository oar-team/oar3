import os
import simplejson as json
from urllib.parse import urlparse

from flask import (g, abort, request, Response)
import requests
from ..cache import cache

from . import Blueprint

from oar.lib import config
from oar.lib.job_handling import get_job

import oar.lib.tools as tools

app = Blueprint('proxy', __name__, url_prefix='/proxy')

if 'OARDIR' not in os.environ:
    os.environ['OARDIR'] = '/usr/local/lib/oar'

OARDODO_CMD = os.environ['OARDIR'] + '/oardodo/oardodo'
if 'OARDODO' in config:
    OARDODO_CMD = config['OARDODO']


@app.route('/<int:job_id>/', defaults={'path': None})
@app.route('/<int:job_id>/<path:path>')
@app.need_authentication()
def proxy(job_id, path):

    if config['OAR_PROXY_INTERNAL'] != 'yes':
        abort(404, 'Proxy is not configured')

    job = get_job(job_id)
    if not job:
        abort(404, 'Job: {} does not exist'.format(job_id))

    user = g.current_user
    if (job.user != user) and (user != 'oar') and (user != 'root'):
        abort(403, 'User {} not allowed'.format(g.current_user))

    proxy_target = cache.get(job_id)
    if not proxy_target:
        proxy_filename = '{}/OAR.{}.proxy.json'.format(job.launching_directory, job.id)

        oar_user_env = os.environ.copy()
        oar_user_env['OARDO_BECOME_USER'] = user

        # Check file's existence
        retcode = tools.call([OARDODO_CMD, 'test', '-e', proxy_filename], env=oar_user_env)
        if retcode:
            abort(404, 'File not found: {}'.format(proxy_filename))

        # Check file's readability
        retcode = tools.call([OARDODO_CMD, 'test', '-r', proxy_filename], env=oar_user_env)
        if retcode:
            abort(403, 'File could not be read: {}'.format(proxy_filename))

        proxy_file_content = tools.check_output([OARDODO_CMD, 'cat', proxy_filename], env=oar_user_env)

        try:
            proxy_target = json.loads(proxy_file_content)
        except Exception as err:
            abort(500, 'Failed to parse {}, error: {}'.format(proxy_filename, err))
        else:
            if 'url' not in proxy_target:
                abort(404, 'Proxy url not in {}: {}'.format(proxy_filename, proxy_target))
            else:
                url_parsed = urlparse(proxy_target['url'])
                if not url_parsed.netloc:
                    abort(404, 'url malformed: {}'.format(proxy_target['url']))
                proxy_target['url'] = 'http://{}/oarapi-priv/'.format(url_parsed.netloc)
            cache.set(job_id, proxy_target)

    resp = requests.request(
        method=request.method,
        url=request.url.replace(request.host_url, proxy_target['url']),
        headers={key: value for (key, value) in request.headers},
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False)

    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    headers = [(name, value) for (name, value) in resp.raw.headers.items()
               if name.lower() not in excluded_headers]

    response = Response(resp.content, resp.status_code, headers)
    return response
