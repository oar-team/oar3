import simplejson as json
from urllib.parse import urlparse

from flask import (g, abort, request, Response)
import requests
from flask_caching import Cache

from . import Blueprint

from oar.lib import config
from oar.lib.job_handling import get_job

app = Blueprint('proxy', __name__)

config = {
    "DEBUG": True,		# some Flask specific configs
    "CACHE_TYPE": "simple",     # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 300
}

app.config.from_mapping(config)
cache = Cache(app)


@app.route('/proxy/<int:job_id>/', defaults={'path': None})
@app.route('/proxy/<int:job_id>/<path:path>')
@app.need_authentication()
def proxy(job_id, path):
    if config['OAR_PROXY_INTERNAL'] != 'yes':
        abort(404, 'Proxy is not configured')

    user = g.current_user
    job = get_job(job_id)
    if (job.user != g.current_user) and (g.current_user != 'oar') and (g.current_user != 'root'):
        abort(403, 'User {} not allowed'.format(g.current_user))

    proxy_target = cache.get(job_id)
    if not proxy_target:
        proxy_filename = '{}/OAR.{}.proxy.json'.forma(job.launching_directory, job.id)
        try:
            json_file = open(proxy_filename)
            proxy_target = json.load(json_file)
        except IOError as err:
            abort(404, 'Failed to read {}, OS error: {}'.format(proxy_filename, err))
        except Exception as err:
            abort(500, 'Failed to parse {}, error: {}'.format(proxy_filename, err))
        else:
            if 'url' not in proxy_target:
                abort(404, 'Proxy url not in {}: {}'.format(proxy_filename, proxy_target))
            else:
                url_parsed = urlparse(proxy_target['url'])
                if not url_parsed.netloc:
                    abort((404, 'url malformed: {}'.format(proxy_target['url']))
                proxy_target['url'] = 'http://{}'.format(url_parsed.netloc)
            cache.set(job_id, proxy_target )

    resp = requests.request(
        method=request.method,
        url=request.url.replace(request.host_url, proxy_target['url']),
        headers={key: value for (key, value) in request.headers if key != 'Host'},
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False)

    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    headers = [(name, value) for (name, value) in resp.raw.headers.items()
               if name.lower() not in excluded_headers]

    response = Response(resp.content, resp.status_code, headers)
    return response
