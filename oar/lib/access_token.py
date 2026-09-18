from datetime import datetime, timedelta
from typing import Optional

from jose import jwt

from oar.lib.configuration import Configuration

# Placeholder value used in the default configuration. Refuse to create or
# validate tokens while it is in use: it is public and would allow anybody to
# forge valid tokens (and impersonate any user, including root).
API_SECRET_KEY_PLACEHOLDER = "TO_CHANGE"


def check_api_secret_key(config) -> str:
    """Check the API secret key is usable, return it.

    Raises a clear error if the key is missing, empty or still set to the
    placeholder value documented in oar.conf.
    """
    secret_key = config.get("API_SECRET_KEY", None)
    if not secret_key or secret_key == API_SECRET_KEY_PLACEHOLDER:
        raise ValueError(
            "The API_SECRET_KEY setting is empty or still set to the placeholder "
            f"value '{API_SECRET_KEY_PLACEHOLDER}'. You MUST set it to a strong "
            "random key before starting the OAR API, otherwise anybody is able to "
            "forge valid API tokens. To generate a key, run: openssl rand -hex 32"
        )
    return secret_key


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
    secret_key = check_api_secret_key(config)
    algorithm = config.get("API_SECRET_ALGORITHM", None)

    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=algorithm)
    return encoded_jwt
