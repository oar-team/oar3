from datetime import datetime

from jose import jwt
from oar.lib.configuration import Configuration

# SECRET_KEY = "3f22a0a65212bfb6cdf0dc4b39be189b3c89c6c2c8ed0d1655e0df837145208b"
ALGORITHM = "HS256"


def create_access_token(data: dict, config: Configuration) -> str:
    to_encode = data.copy()
    to_encode.update({"date": f"{datetime.utcnow()}"})

    # to get a string like this run:
    # openssl rand -hex 32
    SECRET_KEY = config.get("API_SECRET_KEY", None)

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
