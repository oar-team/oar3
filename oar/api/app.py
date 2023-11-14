import os
from typing import Optional

from fastapi import FastAPI, Request, Response
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.lib.configuration import Configuration
from oar.lib.globals import init_config, init_oar

from .routers import frontend, job, media, proxy, resource, stress_factor

SECRET_KEY = "3f22a0a65212bfb6cdf0dc4b39be189b3c89c6c2c8ed0d1655e0df837145208b"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# from oar.api import API_VERSION

default_config = {
    "API_TRUST_IDENT": 1,
    "API_DEFAULT_DATA_STRUCTURE": "simple",
    "API_DEFAULT_MAX_ITEMS_NUMBER": 500,
    "API_ABSOLUTE_URIS": 1,
}


class WSGIProxyFix(object):
    def __init__(self, app, config):
        self.app = app
        self.config = config

    def __call__(self, scope, receive, send):
        user = os.environ.get("AUTHENTICATE_UID", None)
        # Activate only for http requests
        if "http" == scope["type"]:
            if user is not None:
                scope["USER"] = user
            else:
                if self.config.get("API_TRUST_IDENT", 0) == 1:
                    # Create a request object to facilitate access to headers
                    request = Request(scope, receive=receive)

                    user = request.headers.get("x-remote-ident", None)
                    if user not in ("", "unknown", "(null)"):
                        scope["USER"] = user

        return self.app(scope, receive, send)


def create_app(
    config: Optional[Configuration] = None, engine=None, root_path: Optional[str] = None, logger=None
):
    """Return the OAR API application instance."""
    app = FastAPI(root_path=root_path)

    if not config:
        config = init_config()

    if engine is None and logger is None:
        config, engine, logger = init_oar(config=config)
    elif engine is None:
        config, engine, _ = init_oar(config=config)

    logger.info("creating app")

    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    config.setdefault_config(default_config)

    app.include_router(frontend.router)
    app.include_router(resource.router)
    app.include_router(job.router)
    app.include_router(proxy.router)
    app.include_router(media.router)
    app.include_router(stress_factor.router)

    @app.middleware("http")
    async def reflect_database(request: Request, call_next):
        response = await call_next(request)
        return response

    @app.middleware("http")
    async def db_session_middleware(request: Request, call_next):
        response = Response("Internal server error", status_code=500)
        try:
            request.state.db = scoped()
            request.state.config = config
            request.state.logger = logger
            response = await call_next(request)
        finally:
            # FIXME: closing the session here causes the ephemeral session to remove all the data
            # leading to breaking tests
            # request.state.db.close()
            pass
        return response

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        current_user = request.scope.get("USER", None)
        if current_user is not None:
            os.environ["OARDO_USER"] = current_user
        else:
            if "OARDO_USER" in os.environ:
                del os.environ["OARDO_USER"]

        # Calls next middleware
        response = await call_next(request)
        return response

    @app.on_event("shutdown")
    def shutdown_db_session():
        # db.session.remove()
        pass

    app.add_middleware(WSGIProxyFix, config=config)
    return app


# app = create_app()
