# -*- coding: utf-8 -*-
from typing import Annotated, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer
from jose import JWTError, jwt

from .dependencies import get_config

oauth2_scheme = HTTPBearer()


# async def need_authentication(x_remote_ident: Optional[str] = Header(None)):
#     if x_remote_ident is None:
#         raise HTTPException(status_code=403)
#     return x_remote_ident


# async def get_user(x_remote_ident: Optional[str] = Header(None)):
#     if x_remote_ident is None:
#         return None
#     return x_remote_ident


def get_user(
    credentials: Annotated[str, Depends(oauth2_scheme)], config=Depends(get_config)
) -> Optional[str]:

    username = None
    token = credentials.credentials

    # FIXME: HAndlre er
    SECRET_KEY = config["API_SECRET_KEY"]
    ALGORITHM = config["API_SECRET_ALGORITHM"]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("user")
    except JWTError as e:
        print(f"error: {e}")

    return username


def need_authentication(user=Depends(get_user)) -> str:
    if not user:
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials !!! ",
            headers={"WWW-Authenticate": "Bearer"},
        )
        raise credentials_exception
    return user
