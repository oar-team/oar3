# -*- coding: utf-8 -*-
import os
from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from passlib.apache import HtpasswdFile

from oar import VERSION
from oar.lib.configuration import Configuration  # ,Request, Header, Depends

from .. import API_VERSION
from ..dependencies import get_config, get_user
from . import TimestampRoute

# from oar.lib import config


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


from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

fake_users_db = {
    "johndoe": {
        "username": "johndoe",
        "full_name": "John Doe",
        "email": "johndoe@example.com",
        "hashed_password": "fakehashedsecret",
        "disabled": False,
    },
    "alice": {
        "username": "alice",
        "full_name": "Alice Wonderson",
        "email": "alice@example.com",
        "hashed_password": "fakehashedalice",
        "disabled": True,
    },
}

class User(BaseModel):
    username: str
    disabled: bool | None = None


def fake_decode_token(token):
    return User(
        username=token + "fakedecoded"
    )


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    user = fake_decode_token(token)
    return user


@router.get("/me")
async def read_users_me(current_user: Annotated[User, Depends(get_current_user)]):
    return current_user


class UserInDB(User):
    hashed_password: str

def fake_hash_password(password: str):
    return "fakehashed" + password


@router.post("/token")
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    user_dict = fake_users_db.get(form_data.username)
    if not user_dict:
        raise HTTPException(status_code=400, detail=f"1: Incorrect username or password {form_data.password}, {form_data.username}")
    user = UserInDB(**user_dict)
    hashed_password = fake_hash_password(form_data.password)
    if not hashed_password == user.hashed_password:
        raise HTTPException(status_code=400, detail=f"2: Incorrect username or password {form_data}")

    return {"access_token": user.username, "token_type": "bearer"}