# -*- coding: utf-8 -*-
from typing import Annotated, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer
from jose import JWTError, jwt

from oar.lib.submission import check_reservation

from .dependencies import get_config, get_revoked_tokens

oauth2_scheme = HTTPBearer()


def get_token_data(
    credentials: Annotated[str, Depends(oauth2_scheme)], config=Depends(get_config)
) -> Optional[dict]:
    token = credentials.credentials

    # FIXME: HAndlre er
    SECRET_KEY = config["API_SECRET_KEY"]
    ALGORITHM = config["API_SECRET_ALGORITHM"]

    payload = None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as e:
        print(f"error: {e}")

    return payload


def need_authentication(
    data=Depends(get_token_data), revoked_tokens=Depends(get_revoked_tokens)
) -> str:
    if not data:
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials !!! ",
            headers={"WWW-Authenticate": "Bearer"},
        )

        raise credentials_exception

    user: str = data.get("user")
    date: str = data.get("date")

    token_revoked = False
    (_, issued_date) = check_reservation(date)

    if "global" in revoked_tokens:
        # Lets assume date are check at init time
        (erro, revocation_date) = check_reservation(revoked_tokens["global"])
        print(erro)

        if revocation_date > issued_date:
            token_revoked = True

    if (
        not token_revoked
        and "revoked" in revoked_tokens
        and user in revoked_tokens["revoked"]
    ):
        (_, revocation_date) = check_reservation(revoked_tokens["revoked"][user])
        if revocation_date > issued_date:
            token_revoked = True

    if token_revoked:
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token not valid anymore (revoked by an admin)",
            headers={"WWW-Authenticate": "Bearer"},
        )
        raise credentials_exception

    if not user:
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials !!! ",
            headers={"WWW-Authenticate": "Bearer"},
        )
        raise credentials_exception

    return user
