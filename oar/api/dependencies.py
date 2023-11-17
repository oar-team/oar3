from fastapi import Request


# Dependency
def get_db(request: Request):
    return request.state.db


def get_logger(request: Request):
    return request.state.logger


def get_revoked_tokens(request: Request):
    return request.state.revoked_tokens


# Dependency
def get_config(request: Request):
    return request.state.config
