from datetime import datetime

from jose import jwt

SECRET_KEY = "3f22a0a65212bfb6cdf0dc4b39be189b3c89c6c2c8ed0d1655e0df837145208b"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


def create_access_token(data: dict):
    to_encode = data.copy()
    to_encode.update({"date": f"{datetime.utcnow()}"})

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
