from datetime import datetime, timedelta
from typing import Optional

from jose import jwt

from oar.lib.configuration import Configuration


def create_access_token(
    data: dict, config: Configuration, now: Optional[datetime] = None
) -> str:
    to_encode = data.copy()

    if now is None:
        now = datetime.utcnow()

    exp_minutes = int(config.get("API_ACCESS_TOKEN_EXPIRE_MINUTES"))
    expires_delta = timedelta(minutes=exp_minutes)

    expire = now + expires_delta

    to_encode.update({"exp": expire, "date": f"{now.strftime('%Y-%m-%d %H:%M:%S')}"})

    # to get a string like this run:
    # openssl rand -hex 32
    secret_key = config.get("API_SECRET_KEY", None)
    algorithm = config.get("API_SECRET_ALGORITHM", None)

    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=algorithm)
    return encoded_jwt
