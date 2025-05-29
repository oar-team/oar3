# -*- coding: utf-8 -*-
import os
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from passlib.apache import HtpasswdFile

from oar import VERSION
from oar.lib.access_token import create_access_token

# from oar.lib.globals import get_logger
from oar.lib.configuration import Configuration

from .. import API_VERSION
from ..auth import get_token_data, need_authentication
from ..dependencies import get_config
from . import TimestampRoute

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
def whoami(data: str = Depends(get_token_data)):
    """Give the name of the authenticated user seen by OAR API.

    The name for a not authenticated user is the null string.
    """
    user = ""

    if data:
        user = data["user"]

    return {"authenticated_user": user}


@router.get("/check_token")
async def read_users_me(
    current_user: Annotated[str, Depends(get_token_data)],
    auth_user: str = Depends(need_authentication),
):
    data = {"user": current_user["user"], "auth": "Token invalid or revoked"}
    if auth_user:
        data["auth"] = "Token valid"

    return data


@router.get("/get_new_token")
async def get_token(
    current_user: Annotated[str, Depends(get_token_data)],
    auth_user: str = Depends(need_authentication),
    config: Configuration = Depends(get_config),
):
    if auth_user:
        token = create_access_token({"user": current_user["user"]}, config)

    return {"OAR_API_TOKEN": token}


@router.get("/timezone")
def timezone():
    """
    Gives the timezone of the OAR API server.
    The api_timestamp given in each query is an UTC timestamp (epoch unix time).
    This timezone information allows you to re-construct the local time.
    Time is send by defaut (see __init__.py)
    """
    return {}


# FIXME: Is it still needed ?
@router.get("/authentication")
def authentication(
    basic_user: str, basic_password: str, config: Configuration = Depends(get_config)
):
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
