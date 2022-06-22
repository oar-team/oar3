# -*- coding: utf-8 -*-
import os

from fastapi import APIRouter, Depends, HTTPException  # ,Request, Header, Depends
from passlib.apache import HtpasswdFile

from oar import VERSION
from oar.lib import config

from .. import API_VERSION
from ..dependencies import get_user
from . import TimestampRoute

# from oar.lib import config

router = APIRouter(
    # prefix="/",
    route_class=TimestampRoute,
    tags=["frontend"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
def index():
    """Get all main url section pages."""
    data = {}
    data["api_version"] = API_VERSION
    data["apilib_version"] = API_VERSION
    data["oar_version"] = VERSION
    data["links"] = []
    # endpoints = ('resources', 'jobs', 'config', 'admission_rules')
    endpoints = ("/resources", "/jobs")
    for endpoint in endpoints:
        data["links"].append(
            {
                "rel": "collection",
                "href": endpoint,
                "title": endpoint,
            }
        )
    return data


@router.get("/version")
def version():
    """Give OAR and OAR API version.
    Also gives the timezone of the API server.
    """
    data = {}
    data["oar_server_version"] = VERSION
    data["oar_version"] = VERSION
    data["oar_lib_version"] = VERSION
    data["api_version"] = API_VERSION
    data["apilib_version"] = API_VERSION

    return data


@router.get("/whoami")
def whoami(user: str = Depends(get_user)):
    """Give the name of the authenticated user seen by OAR API.

    The name for a not authenticated user is the null string.
    """
    return {"authenticated_user": user}


@router.get("/timezone")
def timezone():
    """
    Gives the timezone of the OAR API server.
    The api_timestamp given in each query is an UTC timestamp (epoch unix time).
    This timezone information allows you to re-construct the local time.
    Time is send by defaut (see __init__.py)
    """
    return {}


@router.get("/authentication")
def authentication(basic_user: str, basic_password: str):
    """allow to test is user/password math htpasswd, can be use as workaround
    to avoid popup open on browser, usefull for integrated dashboard"""

    htpasswd_filename = None
    if not (basic_user and basic_password):
        raise HTTPException(
            status_code=400, detail="Basic authentication is not provided"
        )

    if "HTPASSWD_FILE" in config:
        htpasswd_filename = config["HTPASSWD_FILE"]
    elif os.path.exists("/etc/oar/api-users"):
        htpasswd_filename = "/etc/oar/api-users"
    else:
        raise HTTPException(
            status_code=404, detail="File for basic authentication is not found"
        )

    ht = HtpasswdFile(htpasswd_filename)

    if ht.check_password(basic_user, basic_password):
        return {"basic authentication": "valid"}

    raise HTTPException(status_code=400, detail="basic authentication is not validated")
