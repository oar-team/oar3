import os
from urllib.parse import urlparse

import simplejson as json
from flask import abort, g, redirect

import oar.lib.tools as tools
from oar.lib import config
from oar.lib.job_handling import get_job

from ..proxy_utils import (
    acquire_lock,
    add_traefik_rule,
    load_traefik_rules,
    release_lock,
    save_treafik_rules,
)
from . import Blueprint

app = Blueprint("proxy", __name__, url_prefix="/proxy")

if "OARDIR" not in os.environ:
    os.environ["OARDIR"] = "/usr/local/lib/oar"

OARDODO_CMD = os.environ["OARDIR"] + "/oardodo/oardodo"
if "OARDODO" in config:
    OARDODO_CMD = config["OARDODO"]


@app.route("/<int:job_id>/", defaults={"path": None})
@app.route("/<int:job_id>/<path:path>")
@app.need_authentication()
def proxy(job_id, path):
    if config["PROXY"] != "traefik":
        abort(404, "Proxy is not configured")

    job = get_job(job_id)
    if not job:
        abort(404, "Job: {} does not exist".format(job_id))

    user = g.current_user
    if (job.user != user) and (user != "oar") and (user != "root"):
        abort(403, "User {} not allowed".format(g.current_user))

    proxy_filename = "{}/OAR.{}.proxy.json".format(job.launching_directory, job.id)

    oar_user_env = os.environ.copy()
    oar_user_env["OARDO_BECOME_USER"] = user

    # Check file's existence
    retcode = tools.call([OARDODO_CMD, "test", "-e", proxy_filename], env=oar_user_env)
    if retcode:
        abort(404, "File not found: {}".format(proxy_filename))

    # Check file's readability
    retcode = tools.call([OARDODO_CMD, "test", "-r", proxy_filename], env=oar_user_env)
    if retcode:
        abort(403, "File could not be read: {}".format(proxy_filename))

    proxy_file_content = tools.check_output(
        [OARDODO_CMD, "cat", proxy_filename], env=oar_user_env
    )

    try:
        proxy_target = json.loads(proxy_file_content)
    except Exception as err:
        abort(500, "Failed to parse {}, error: {}".format(proxy_filename, err))
    else:
        if "url" not in proxy_target:
            abort(404, "Proxy url not in {}: {}".format(proxy_filename, proxy_target))
        else:
            url_parsed = urlparse(proxy_target["url"])

            if not url_parsed.netloc:
                abort(404, "url malformed: {}".format(proxy_target["url"]))

            if url_parsed.scheme != "http":
                abort(
                    404,
                    "only http scheme url is supported: {}".format(url_parsed.scheme),
                )

            url_base_target = "{}://{}".format(url_parsed.scheme, url_parsed.netloc)

            prefix_url = "{}/{}".format(config["OAR_PROXY_BASE_URL"], job_id)

            lock_fd = acquire_lock()
            proxy_rules_filename = config["PROXY_TRAEFIK_RULES_FILE"]
            try:
                rules = load_traefik_rules(proxy_rules_filename)
                add_traefik_rule(rules, prefix_url, url_base_target)
                save_treafik_rules(proxy_rules_filename, rules)
            except Exception as err:
                release_lock(lock_fd)
                abort(
                    500,
                    "Failed to set proxy rules for job: {} Error {}".format(
                        job_id, err
                    ),
                )

            release_lock(lock_fd)
            return redirect(
                "{}{}".format(config["PROXY_TRAEFIK_ENTRYPOINT"], prefix_url)
            )
