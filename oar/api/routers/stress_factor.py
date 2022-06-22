# -*- coding: utf-8 -*-

"""
oar.rest_api.views.stress_factor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Define stress_factor retreiving api

"""
import os

from fastapi import APIRouter, HTTPException

import oar.lib.tools as tools
from oar.lib import config

from . import TimestampRoute

router = APIRouter(
    route_class=TimestampRoute,
    prefix="/stress_factor",
    tags=["stress_factor"],
    responses={404: {"description": "Not found"}},
)

if "OARDIR" not in os.environ:
    os.environ["OARDIR"] = "/usr/local/lib/oar"

OARDODO_CMD = os.environ["OARDIR"] + "/oardodo/oardodo"
if "OARDODO" in config:
    OARDODO_CMD = config["OARDODO"]


@router.get("/")
def index():
    stress_factor_script = "/etc/oar/stress_factor.sh"
    if "API_STRESS_FACTOR_SCRIPT" in config:
        stress_factor_script = config["API_STRESS_FACTOR_SCRIPT"]

    cmd = [OARDODO_CMD, "bash", "--noprofile", "--norc", "-c", stress_factor_script]

    stress_factor_result = tools.check_output(cmd).decode().split("\n")

    global_stress = None
    stress_factor_value = ""
    data = {}
    for sf_res in stress_factor_result:
        sf = sf_res.split("=")
        if (len(sf) == 2) and (sf[0] == "GLOBAL_STRESS"):
            global_stress = sf[0]
            stress_factor_value = sf[1]
    if global_stress:
        data[global_stress] = stress_factor_value
        url = router.prefix
        data["links"] = [{"rel": "rel", "href": url}]

    else:
        raise HTTPException(status_code=404, detail="Unable to retrieve STRESS_FACTOR")
    return data
