from typing import Optional

from fastapi import Header, HTTPException, Request


async def need_authentication(x_remote_ident: Optional[str] = Header(None)):
    if x_remote_ident is None:
        raise HTTPException(status_code=403)
    return x_remote_ident


async def get_user(x_remote_ident: Optional[str] = Header(None)):
    if x_remote_ident is None:
        return None
    return x_remote_ident


# Dependency
def get_db(request: Request):
    print("get_db: ", request.state.db)
    return request.state.db


# Dependency
def get_config(request: Request):
    return request.state.config
