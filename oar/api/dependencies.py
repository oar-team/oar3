from fastapi import Request


# Dependency
def get_db(request: Request):
    return request.state.db


def get_logger(request: Request):
    return request.state.logger


# Dependency
def get_config(request: Request):
    return request.state.config
