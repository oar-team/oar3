import os
from urllib.parse import urlparse

import simplejson as json
from fastapi import APIRouter, Depends, HTTPException
from starlette.responses import RedirectResponse

import oar.lib.tools as tools
from oar.lib import config
from oar.lib.job_handling import get_job

from ..dependencies import need_authentication
from ..proxy_utils import (
    acquire_lock,
    add_traefik_rule,
    load_traefik_rules,
    release_lock,
    save_treafik_rules,
)
from . import TimestampRoute

router = APIRouter(
    route_class=TimestampRoute,
    prefix="/proxy",
    tags=["proxy"],
    responses={404: {"description": "Not found"}},
)


if "OARDIR" not in os.environ:
    os.environ["OARDIR"] = "/usr/local/lib/oar"

OARDODO_CMD = os.environ["OARDIR"] + "/oardodo/oardodo"
if "OARDODO" in config:
    OARDODO_CMD = config["OARDODO"]


@router.get("/{job_id}")
def proxy(job_id: int, user=Depends(need_authentication)):
    if config["PROXY"] != "traefik":
        raise HTTPException(status_code=404, detail="Proxy is not configured")

    job = get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=404, detail="Job: {} does not exist".format(job_id)
        )

    if (job.user != user) and (user != "oar") and (user != "root"):
        raise HTTPException(status_code=403, detail="User {} not allowed".format(user))

    proxy_filename = "{}/OAR.{}.proxy.json".format(job.launching_directory, job.id)

    oar_user_env = os.environ.copy()
    oar_user_env["OARDO_BECOME_USER"] = user

    # Check file's existence
    retcode = tools.call([OARDODO_CMD, "test", "-e", proxy_filename], env=oar_user_env)
    if retcode:
        raise HTTPException(
            status_code=404, detail="File not found: {}".format(proxy_filename)
        )

    # Check file's readability
    retcode = tools.call([OARDODO_CMD, "test", "-r", proxy_filename], env=oar_user_env)
    if retcode:
        raise HTTPException(
            status_code=403, detail="File could not be read: {}".format(proxy_filename)
        )

    proxy_file_content = tools.check_output(
        [OARDODO_CMD, "cat", proxy_filename], env=oar_user_env
    )

    try:
        proxy_target = json.loads(proxy_file_content)
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail="Failed to parse {}, error: {}".format(proxy_filename, err),
        )

    else:
        if "url" not in proxy_target:
            raise HTTPException(
                status_code=404,
                detail="Proxy url not in {}: {}".format(proxy_filename, proxy_target),
            )

        else:
            url_parsed = urlparse(proxy_target["url"])

            if not url_parsed.netloc:
                raise HTTPException(
                    status_code=404,
                    detail="url malformed: {}".format(proxy_target["url"]),
                )

            if url_parsed.scheme != "http":
                raise HTTPException(
                    status_code=404,
                    detail="only http scheme url is supported: {}".format(
                        url_parsed.scheme
                    ),
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
                raise HTTPException(
                    status_code=500,
                    detail="Failed to set proxy rules for job: {} Error {}".format(
                        job_id, err
                    ),
                )

            release_lock(lock_fd)
            return RedirectResponse(
                url="{}{}".format(config["PROXY_TRAEFIK_ENTRYPOINT"], prefix_url)
            )
