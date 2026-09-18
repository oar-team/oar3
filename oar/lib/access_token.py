from datetime import datetime, timedelta
from typing import Optional

from jose import jwt

from oar.lib.configuration import Configuration

# Secret values that must never be used to sign/validate tokens.
# API_SECRET_KEY_PLACEHOLDER is the documented value in oar.conf,
# API_SECRET_KEY_LEGACY_DEFAULT is the key shipped by default in older OAR
# versions: it has been publicly known since then and must be regenerated too.
API_SECRET_KEY_PLACEHOLDER = "TO_CHANGE"
API_SECRET_KEY_LEGACY_DEFAULT = (
    "3f22a0a65212bfb6cdf0dc4b39be189b3c89c6c2c8ed0d1655e0df837145208b"
)


def check_api_secret_key(config) -> str:
    """Check the API secret key is usable, return it.

    Raises a clear error if the key is missing, empty, too short or still set
    to a known/default value (the placeholder documented in oar.conf or the
    key that was shipped as default in older versions).
    """
    secret_key = config.get("API_SECRET_KEY", None)
    if (
        not secret_key
        or secret_key in (API_SECRET_KEY_PLACEHOLDER, API_SECRET_KEY_LEGACY_DEFAULT)
        or len(secret_key) < 16
    ):
        raise ValueError(
            "The API_SECRET_KEY setting is empty, too short, or still set to a "
            f"known/default value (e.g. the '{API_SECRET_KEY_PLACEHOLDER}' "
            "placeholder or the key documented in older OAR versions). You MUST "
            "set it to a strong random key before starting the OAR API, otherwise "
            "anybody is able to forge valid API tokens. To generate a key, run: "
            "openssl rand -hex 32"
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
